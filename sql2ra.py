import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Comparison
from sqlparse.tokens import Keyword, DML, Wildcard, Operator, String, Name
import radb
import radb.ast
import radb.parse



def extract_select_columns(stmt):
    """Extracts column definitions for Projection."""
    columns = []
    select_seen = False
    
    for token in stmt.tokens:
        if token.ttype is DML and token.value.upper() == 'SELECT':
            select_seen = True
            continue
        if select_seen and token.ttype is Keyword and token.value.upper() == 'FROM':
            break
        if select_seen and token.ttype is Keyword and token.value.upper() == 'DISTINCT':
            continue
            
        if select_seen and not token.is_whitespace:
            # Case 1: multiple columns — e.g., "name, age"
            if isinstance(token, IdentifierList):
                for id_token in token.get_identifiers():
                    columns.append(id_token)
            elif isinstance(token, Identifier):
                columns.append(token)
            elif token.ttype is Wildcard:
                return '*'
                
    if not columns:
        return '*'

#The RA operator for projection, which i will use later requires a list of radb.ast.AttrRef objects as its attribute list.
# If you tried to pass the original sqlparse tokens, the radb library would throw a type error and crash 
# because it wouldn't know how to interpret a generic token as a database attribute.
# that's why we're gonna make list of radb.ast.AttrRef objects
    attributes = []
    for col in columns:
        # 1. Check if it's a complex token (like Person.name)
        if hasattr(col, 'get_parent_name'):
            table_name = col.get_parent_name() # Extracts "Person" from "Person.name"
            column_name = col.get_real_name() # Extracts "name" (removes quotes/aliases)
        # 2. Handle simple tokens (like age)
        else:
            table_name = None
            column_name = col.value
        # 3. Create the object radb requires
        attributes.append(radb.ast.AttrRef(table_name, column_name))
        
    return attributes

def extract_from_tables(stmt):
    """Extracts tables and aliases for Cross Product/Renaming."""
    tables = []
    from_seen = False
    
    for token in stmt.tokens:
        if token.ttype is Keyword and token.value.upper() == 'FROM':
            from_seen = True
            continue
        # Stop at WHERE or ;
        if isinstance(token, Where):
            break
            
        if from_seen and not token.is_whitespace:
            if isinstance(token, IdentifierList):
                for ident in token.get_identifiers():
                    tables.append((ident.get_real_name(), ident.get_alias()))
            elif isinstance(token, Identifier):
                tables.append((token.get_real_name(), token.get_alias()))
            else:
                # Fallback for simple names not wrapped in Identifier
                name = token.value
                tables.append((name, None))
                
    return tables

def get_condition_ast(stmt):
    """
    Extracts WHERE condition and converts it to a radb AST.
    Uses sqlparse tokens to safely reconstruct the string for radb.
    """
    # 1. Find the WHERE clause
    where_token = None
    for token in stmt.tokens:
        if isinstance(token, Where):
            where_token = token
            break
            
    if not where_token:
        return None

    # 2. Reconstruct the condition string using sqlparse classifications
    # We use flatten() to get a linear list of all atomic tokens
    # This avoids us having to manually parse nested Identifier structures
    tokens = where_token.flatten()
    
    clean_parts = []
    
    for token in tokens:
        # Skip the actual "WHERE" keyword itself
        if token.ttype is Keyword and token.value.upper() == 'WHERE':
            continue
            
        # Skip whitespace (we will add our own clean spacing)
        if token.is_whitespace:
            continue
            
        # Handle Logic Operators (AND, OR) - radb needs lowercase
        if token.ttype is Keyword and token.value.upper() in ('AND', 'OR'):
            clean_parts.append(token.value.lower())
            
        # Handle Comparison Operators (=, <, >) - radb needs spaces around them
        elif token.ttype is Operator.Comparison:
            clean_parts.append(f" {token.value} ")
            
        # Handle String Literals - Keep them exactly as is (preserves quotes)
        elif token.ttype is String.Single:
            clean_parts.append(token.value)
            
        # Handle everything else (Column names, numbers, dots)
        else:
            clean_parts.append(token.value)

    # Join with spaces to ensure separation
    # Note: "Table . Column" is valid in radb parsing, so joining with space is safe
    clean_cond = " ".join(clean_parts)
    
    # 3. Use the dummy statement trick to parse the RA condition
    dummy_stmt = f"\\select_{{{clean_cond}}}(Dummy);"
    
    try:
        parsed_node = radb.parse.one_statement_from_string(dummy_stmt)
        return parsed_node.cond
    except Exception:
        return None

def translate(sql_input):
    """
    Translates SQL to Relational Algebra AST.
    """
    if isinstance(sql_input, str):
        stmt = sqlparse.parse(sql_input)[0]
    else:
        stmt = sql_input

    # 1. Processing FROM: Cross Products and Renames
    tables = extract_from_tables(stmt)
    rel_nodes = []
    for name, alias in tables:
        node = radb.ast.RelRef(name)
        if alias and alias != name:
            # Generate \rename_{Alias: *}(Rel)
            node = radb.ast.Rename(alias, None, node)
        rel_nodes.append(node)
        
    # Build tree: ((T1 cross T2) cross T3)...
    ra = rel_nodes[0]
    for next_rel in rel_nodes[1:]:
        ra = radb.ast.Cross(ra, next_rel)

    # 2. Processing WHERE: Selection
    cond_ast = get_condition_ast(stmt)
    if cond_ast:
        ra = radb.ast.Select(cond_ast, ra)

    # 3. Processing SELECT: Projection
    cols = extract_select_columns(stmt)
    if cols != '*':
        ra = radb.ast.Project(cols, ra)

    return ra