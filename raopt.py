import radb
import radb.ast
import radb.parse

def get_attrs(cond):
    attrs = set()
    if isinstance(cond, radb.ast.AttrRef):
        if cond.rel: 
            attrs.add(f"{cond.rel}.{cond.name}")
        else: 
            attrs.add(cond.name)
        return attrs
    if hasattr(cond, 'inputs'):
        for child in cond.inputs: 
            attrs.update(get_attrs(child))
    return attrs

def get_rel_names(ra):
    """Recursively finds all table names and aliases in a subtree."""
    names = set()
    if isinstance(ra, radb.ast.RelRef):
        names.add(ra.rel.upper())
    elif isinstance(ra, radb.ast.Rename):
        if ra.relname:
            names.add(ra.relname.upper())
        names.update(get_rel_names(ra.inputs[0]))
    elif hasattr(ra, 'inputs'):
        for i in ra.inputs:
            names.update(get_rel_names(i))
    return names

def get_schema(ra, dd):
    if isinstance(ra, radb.ast.RelRef):
        for table_name in dd:
            if table_name.upper() == ra.rel.upper():
                return {f"{table_name}.{attr}" for attr in dd[table_name]}
        return None 
    elif isinstance(ra, radb.ast.Rename):
        child_schema = get_schema(ra.inputs[0], dd)
        if child_schema is None: return None
        new_schema = set()
        if ra.relname:
            for attr in child_schema:
                col = attr.split('.')[-1]
                new_schema.add(f"{ra.relname}.{col}")
        else: 
            new_schema = child_schema
        return new_schema
    elif isinstance(ra, (radb.ast.Cross, radb.ast.Join)):
        s1 = get_schema(ra.inputs[0], dd)
        s2 = get_schema(ra.inputs[1], dd)
        if s1 is None or s2 is None: return None
        return s1 | s2
    elif isinstance(ra, (radb.ast.Select, radb.ast.Project)):
        return get_schema(ra.inputs[0], dd)
    return None

def matches_schema(attrs, schema):
    if schema is None: return True
    schema_upper = {s.upper() for s in schema}
    # Extract table names from schema (e.g., 'PERSON' from 'PERSON.NAME')
    schema_tables = set()
    for s in schema_upper:
        if '.' in s:
            schema_tables.add(s.split('.')[0])
    
    for attr in attrs:
        attr_up = attr.upper()
        # Direct match (e.g., 'PERSON.NAME' in schema)
        if attr_up in schema_upper: 
            continue
        # Check if unqualified attribute matches (e.g., 'NAME' matches 'PERSON.NAME')
        if '.' not in attr_up:
            # Unqualified attribute - check if any schema column ends with this name
            found = False
            for s in schema_upper:
                if s.endswith('.' + attr_up):
                    found = True
                    break
            if found:
                continue
        else:
            # Qualified attribute (e.g., 'EATS.NAME') - check if table is in schema
            attr_table = attr_up.split('.')[0]
            if attr_table not in schema_tables:
                # Table not in schema, attribute doesn't match
                return False
            # Table is in schema, check if full attribute matches
            found = False
            for s in schema_upper:
                if s == attr_up:
                    found = True
                    break
            if found:
                continue
        # Attribute not found in schema
        return False
    return True

def rewrite_cond_for_schema(cond, alias, child_schema):
    if child_schema is None: return cond 
    if isinstance(cond, radb.ast.AttrRef):
        if cond.rel == alias:
            target_col = cond.name.upper()
            for s in child_schema:
                parts = s.split('.')
                col_name = parts[-1]
                if col_name.upper() == target_col:
                    if '.' in s:
                        r, c = s.split('.', 1)
                        return radb.ast.AttrRef(r, c)
                    else:
                        return radb.ast.AttrRef(None, s)
        return cond
    if isinstance(cond, radb.ast.ValExprBinaryOp):
        return radb.ast.ValExprBinaryOp(
            rewrite_cond_for_schema(cond.inputs[0], alias, child_schema),
            cond.op,
            rewrite_cond_for_schema(cond.inputs[1], alias, child_schema)
        )
    return cond

def rule_break_up_selections(ra):
    if isinstance(ra, radb.ast.RelRef): return ra
    if hasattr(ra, 'inputs'): 
        for i in range(len(ra.inputs)): ra.inputs[i] = rule_break_up_selections(ra.inputs[i])
    if isinstance(ra, radb.ast.Select) and isinstance(ra.cond, radb.ast.ValExprBinaryOp): 
        op = ra.cond.op
        is_and = (isinstance(op, str) and op.lower() == 'and') or \
                 (hasattr(radb.parse.RAParser, 'AND') and op == radb.parse.RAParser.AND)
        if is_and:
            cond1 = ra.cond.inputs[0]
            cond2 = ra.cond.inputs[1]
            return rule_break_up_selections(radb.ast.Select(cond1, radb.ast.Select(cond2, ra.inputs[0])))
    return ra

def rule_push_down_selections(ra, dd):
    if isinstance(ra, radb.ast.RelRef): return ra
    if hasattr(ra, 'inputs'):
        for i in range(len(ra.inputs)): 
            ra.inputs[i] = rule_push_down_selections(ra.inputs[i], dd)
    if isinstance(ra, radb.ast.Select):
        child = ra.inputs[0]
        cond_attrs = get_attrs(ra.cond)
        if isinstance(child, radb.ast.Select):
            swapped_node = radb.ast.Select(ra.cond, child.inputs[0])
            child.inputs[0] = rule_push_down_selections(swapped_node, dd)
            return child
        if isinstance(child, radb.ast.Project):
            pushed_select = radb.ast.Select(ra.cond, child.inputs[0])
            child.inputs[0] = rule_push_down_selections(pushed_select, dd)
            return child
        if isinstance(child, radb.ast.Rename):
            if child.relname: 
                child_schema = get_schema(child.inputs[0], dd)
                if child_schema:
                    new_cond = rewrite_cond_for_schema(ra.cond, child.relname, child_schema)
                    pushed_select = radb.ast.Select(new_cond, child.inputs[0])
                    child.inputs[0] = rule_push_down_selections(pushed_select, dd)
                    return child
        if isinstance(child, (radb.ast.Cross, radb.ast.Join)):
            left_schema = get_schema(child.inputs[0], dd)
            right_schema = get_schema(child.inputs[1], dd)
            if left_schema and right_schema:
                pushes_left = matches_schema(cond_attrs, left_schema)
                pushes_right = matches_schema(cond_attrs, right_schema)
                if pushes_left:
                    child.inputs[0] = rule_push_down_selections(radb.ast.Select(ra.cond, child.inputs[0]), dd)
                    return child 
                elif pushes_right:
                    child.inputs[1] = rule_push_down_selections(radb.ast.Select(ra.cond, child.inputs[1]), dd)
                    return child
    return ra

def rule_merge_selections(ra):
    if isinstance(ra, radb.ast.Select) and isinstance(ra.inputs[0], radb.ast.Select):
        child = ra.inputs[0]
        new_cond = radb.ast.ValExprBinaryOp(ra.cond, radb.parse.RAParser.AND, child.cond)
        return rule_merge_selections(radb.ast.Select(new_cond, child.inputs[0]))
    if hasattr(ra, 'inputs'):
        for i in range(len(ra.inputs)): ra.inputs[i] = rule_merge_selections(ra.inputs[i])
    return ra

def rule_introduce_joins(ra):
    if isinstance(ra, radb.ast.RelRef): return ra
    if hasattr(ra, 'inputs'):
        for i in range(len(ra.inputs)): ra.inputs[i] = rule_introduce_joins(ra.inputs[i])
    if isinstance(ra, radb.ast.Select):
        child = ra.inputs[0]
        if isinstance(child, radb.ast.Cross):
            return radb.ast.Join(child.inputs[0], ra.cond, child.inputs[1])
    return ra

def rule_push_down_projections(ra, dd, needed=None):
    """
    Push down projections to reduce data early.
    
    Strategy:
    - For single-table queries: Always push projections
    - For 3+ table joins: Always push projections
    - For 2-table joins: Only push if there's a selection with equality on a key column
    """
    if isinstance(ra, radb.ast.Project):
        new_needed = set()
        for attr in ra.attrs:
            new_needed.update(get_attrs(attr))
        
        num_tables = _count_tables(ra.inputs[0])
        
        if num_tables >= 3:
            # Multi-table join - always push projections
            ra.inputs[0] = rule_push_down_projections(ra.inputs[0], dd, new_needed)
        elif num_tables == 2 and _has_key_equality_selection(ra.inputs[0]):
            # 2-table join with highly selective filter - push projections
            ra.inputs[0] = rule_push_down_projections(ra.inputs[0], dd, new_needed)
        elif not _contains_join(ra.inputs[0]):
            # Single-table query - push projections
            ra.inputs[0] = rule_push_down_projections(ra.inputs[0], dd, new_needed)
        else:
            # 2-table join without key filter - don't push (overhead > benefit)
            ra.inputs[0] = rule_push_down_projections(ra.inputs[0], dd, None)
        return ra

    if isinstance(ra, radb.ast.RelRef):
        # If needed is None, don't push any projections
        if needed is None: return ra
        
        schema = get_schema(ra, dd)
        if schema is None: return ra 

        to_project = []
        needed_upper = {n.upper() for n in needed}
        
        for s in schema:
            s_up = s.upper()
            s_col = s_up.split('.')[-1]
            if s_up in needed_upper or s_col in needed_upper:
                parts = s.split('.')
                to_project.append(radb.ast.AttrRef(parts[0], parts[1]) if len(parts) > 1 else radb.ast.AttrRef(None, parts[0]))
        
        # Explicitly check for requested tables based on name prefix (Safety Override)
        my_name = ra.rel.upper()
        for n in needed_upper:
            if "." in n:
                tbl, col = n.split(".", 1)
                if tbl == my_name:
                    # Check if we already added it to avoid duplicates
                    exists = False
                    for existing in to_project:
                        e_name = existing.name.upper()
                        e_rel = existing.rel.upper() if existing.rel else ""
                        if e_name == col and (not e_rel or e_rel == tbl): exists = True
                    if not exists:
                        to_project.append(radb.ast.AttrRef(tbl, col))

        if len(to_project) == len(schema): return ra
        if not to_project: return ra
        return radb.ast.Project(to_project, ra)

    if isinstance(ra, radb.ast.Rename):
        # If needed is None, just recurse without pushing projections
        if needed is None:
            ra.inputs[0] = rule_push_down_projections(ra.inputs[0], dd, None)
            return ra
            
        child_schema = get_schema(ra.inputs[0], dd)
        reqs = needed
        child_needed = set()
        if child_schema is None:
            ra.inputs[0] = rule_push_down_projections(ra.inputs[0], dd, None)
            return ra
        for attr in reqs:
            if "." in attr:
                rel, col = attr.split(".", 1)
                if rel == ra.relname:
                    col_upper = col.upper()
                    for child_attr in child_schema:
                        if child_attr.upper().endswith("." + col_upper) or child_attr.upper() == col_upper:
                            child_needed.add(child_attr); break
                else: child_needed.add(attr)
            else:
                attr_upper = attr.upper()
                found = False
                for child_attr in child_schema:
                    if child_attr.upper().endswith("." + attr_upper):
                        child_needed.add(child_attr); found = True; break
                if not found: child_needed.add(attr)
        ra.inputs[0] = rule_push_down_projections(ra.inputs[0], dd, child_needed)
        return ra

    if isinstance(ra, radb.ast.Select):
        # If needed is None, just recurse without adding projection requirements
        if needed is None:
            ra.inputs[0] = rule_push_down_projections(ra.inputs[0], dd, None)
            return ra
        new_needed = set(needed)
        new_needed |= get_attrs(ra.cond)
        ra.inputs[0] = rule_push_down_projections(ra.inputs[0], dd, new_needed)
        return ra

    if isinstance(ra, (radb.ast.Join, radb.ast.Cross)):
        # If needed is None, just recurse without pushing projections
        if needed is None:
            ra.inputs[0] = rule_push_down_projections(ra.inputs[0], dd, None)
            ra.inputs[1] = rule_push_down_projections(ra.inputs[1], dd, None)
            return ra
        
        # Push projections to both sides of the join
        all_req = set(needed)
        cond_attrs = set()
        if isinstance(ra, radb.ast.Join): 
            cond_attrs = get_attrs(ra.cond)
            all_req |= cond_attrs
        
        schema_l = get_schema(ra.inputs[0], dd)
        schema_r = get_schema(ra.inputs[1], dd)
        
        names_l = get_rel_names(ra.inputs[0])
        names_r = get_rel_names(ra.inputs[1])

        left_needed = set()
        right_needed = set()
        
        for req in all_req:
            in_left = matches_schema({req}, schema_l)
            in_right = matches_schema({req}, schema_r)
            
            if "." in req:
                tbl, col = req.split(".", 1)
                tbl = tbl.upper()
                if tbl in names_l: in_left = True
                if tbl in names_r: in_right = True

            if in_left: left_needed.add(req)
            if in_right: right_needed.add(req)
            
            if not in_left and not in_right:
                left_needed.add(req)
                right_needed.add(req)

        if cond_attrs:
            left_needed |= cond_attrs
            right_needed |= cond_attrs

        ra.inputs[0] = rule_push_down_projections(ra.inputs[0], dd, left_needed)
        ra.inputs[1] = rule_push_down_projections(ra.inputs[1], dd, right_needed)
        return ra

    return ra

def _count_tables(ra):
    """Count the number of base tables (RelRef) in the RA tree."""
    if isinstance(ra, radb.ast.RelRef):
        return 1
    count = 0
    if hasattr(ra, 'inputs'):
        for child in ra.inputs:
            count += _count_tables(child)
    return count

def _contains_join(ra):
    """Check if the RA tree contains any Join or Cross operations."""
    if isinstance(ra, (radb.ast.Join, radb.ast.Cross)):
        return True
    if hasattr(ra, 'inputs'):
        for child in ra.inputs:
            if _contains_join(child):
                return True
    return False

def _has_key_equality_selection(ra):
    """
    Check if the RA tree has a selection with equality on a likely key column.
    This indicates a highly selective filter that would benefit from early projection.
    
    Key columns are identified by common naming patterns like *KEY*, *ID*, etc.
    """
    if isinstance(ra, radb.ast.Select):
        # Check if condition has equality on a key-like column
        if _is_key_equality(ra.cond):
            return True
        # Also check child
        return _has_key_equality_selection(ra.inputs[0])
    if hasattr(ra, 'inputs'):
        for child in ra.inputs:
            if _has_key_equality_selection(child):
                return True
    return False

def _is_key_equality(cond):
    """Check if condition is an equality comparison involving a key-like column."""
    if isinstance(cond, radb.ast.ValExprBinaryOp):
        # Check for AND - recurse on both sides
        if cond.op == radb.parse.RAParser.AND:
            return _is_key_equality(cond.inputs[0]) or _is_key_equality(cond.inputs[1])
        # Check for equality
        if cond.op == radb.parse.RAParser.EQ:
            left, right = cond.inputs[0], cond.inputs[1]
            # Check if either side is a key-like attribute with a literal value
            left_is_key = _is_key_attr(left)
            right_is_key = _is_key_attr(right)
            left_is_literal = isinstance(left, radb.ast.RANumber) or isinstance(left, radb.ast.RAString)
            right_is_literal = isinstance(right, radb.ast.RANumber) or isinstance(right, radb.ast.RAString)
            
            # Key = literal is highly selective
            if (left_is_key and right_is_literal) or (right_is_key and left_is_literal):
                return True
    return False

def _is_key_attr(expr):
    """Check if expression is an attribute that looks like a key column."""
    if isinstance(expr, radb.ast.AttrRef):
        name = expr.name.upper()
        # Common key column patterns
        if 'KEY' in name or 'ID' in name or name.endswith('NO') or name.endswith('NUM'):
            return True
    return False