from enum import Enum
import json
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
from luigi.mock import MockTarget
import radb
import radb.ast
import radb.parse

class ExecEnv(Enum):
    LOCAL = 1
    HDFS = 2
    MOCK = 3

class OutputMixin(luigi.Task):
    exec_environment = luigi.EnumParameter(enum=ExecEnv, default=ExecEnv.HDFS)
    def get_output(self, fn):
        if self.exec_environment == ExecEnv.HDFS: return luigi.contrib.hdfs.HdfsTarget(fn)
        elif self.exec_environment == ExecEnv.MOCK: return MockTarget(fn)
        else: return luigi.LocalTarget(fn)

class InputData(OutputMixin):
    filename = luigi.Parameter()
    def output(self): return self.get_output(self.filename)

def count_steps(raquery):
    if isinstance(raquery, (radb.ast.Select, radb.ast.Project, radb.ast.Rename)):
        return 1 + count_steps(raquery.inputs[0])
    elif isinstance(raquery, (radb.ast.Join, radb.ast.Cross)):
        return 1 + count_steps(raquery.inputs[0]) + count_steps(raquery.inputs[1])
    return 1

class RelAlgQueryTask(luigi.contrib.hadoop.JobTask, OutputMixin):
    querystring = luigi.Parameter()
    step = luigi.IntParameter(default=1)
    def output(self):
        ext = ".tmp" if self.exec_environment != ExecEnv.HDFS else ""
        return self.get_output(f"tmp{self.step}{ext}")

def task_factory(raquery, step=1, env=ExecEnv.HDFS, optimize=False):
    if optimize: return build_optimized_task(raquery, step, env)
    if isinstance(raquery, radb.ast.Select): return SelectTask(querystring=str(raquery) + ";", step=step, exec_environment=env)
    elif isinstance(raquery, radb.ast.RelRef): return InputData(filename=raquery.rel + ".json", exec_environment=env)
    elif isinstance(raquery, radb.ast.Join): return JoinTask(querystring=str(raquery) + ";", step=step, exec_environment=env)
    elif isinstance(raquery, radb.ast.Cross): return CrossTask(querystring=str(raquery) + ";", step=step, exec_environment=env)
    elif isinstance(raquery, radb.ast.Project): return ProjectTask(querystring=str(raquery) + ";", step=step, exec_environment=env)
    elif isinstance(raquery, radb.ast.Rename): return RenameTask(querystring=str(raquery) + ";", step=step, exec_environment=env)
    else: raise Exception(f"Operator {type(raquery)} not implemented.")

def parse_val(data, target):
    if isinstance(target, radb.ast.AttrRef):
        key = f"{target.rel}.{target.name}" if target.rel else target.name
        if key in data: return data[key]
        if target.name in data: return data[target.name]
        suffix = "." + target.name
        for k, v in data.items():
            if k.endswith(suffix): return v
        if not target.rel:
            for k, v in data.items():
                if '.' in k and k.split('.')[-1] == target.name:
                    return v
    elif isinstance(target, radb.ast.Literal):
        val = target.val
        if isinstance(val, str) and ((val.startswith("'") and val.endswith("'")) or (val.startswith('"') and val.endswith('"'))):
            val = val[1:-1]
        return val
    return None

def check_condition(row, condition):
    if isinstance(condition, radb.ast.ValExprBinaryOp):
        op = str(getattr(condition, 'op', '')).strip().upper()
        if hasattr(radb.parse.RAParser, 'EQ') and condition.op == radb.parse.RAParser.EQ: op = '='
        if hasattr(radb.parse.RAParser, 'AND') and condition.op == radb.parse.RAParser.AND: op = 'AND'
        if op == 'AND': return check_condition(row, condition.inputs[0]) and check_condition(row, condition.inputs[1])
        val_l = parse_val(row, condition.inputs[0])
        val_r = parse_val(row, condition.inputs[1])
        if op in ['=', 'EQ', '==']: 
            try:
                if float(val_l) == float(val_r): return True
            except (ValueError, TypeError):
                pass
            return str(val_l) == str(val_r)
    return True

def get_join_attrs(cond):
    pairs = []
    if isinstance(cond, radb.ast.ValExprBinaryOp):
        op = str(getattr(cond, 'op', '')).strip().upper()
        if hasattr(radb.parse.RAParser, 'EQ') and cond.op == radb.parse.RAParser.EQ: op = '='
        if hasattr(radb.parse.RAParser, 'AND') and cond.op == radb.parse.RAParser.AND: op = 'AND'
        if op == 'AND':
            pairs.extend(get_join_attrs(cond.inputs[0]))
            pairs.extend(get_join_attrs(cond.inputs[1]))
        elif op in ['=', 'EQ']:
            if isinstance(cond.inputs[0], radb.ast.AttrRef) and isinstance(cond.inputs[1], radb.ast.AttrRef):
                pairs.append((cond.inputs[0], cond.inputs[1]))
    return pairs

def contains_join(ra):
    if isinstance(ra, (radb.ast.Join, radb.ast.Cross)): return True
    if hasattr(ra, 'inputs'):
        for child in ra.inputs:
            if contains_join(child): return True
    return False

def get_base_relation(ra):
    if isinstance(ra, radb.ast.RelRef): return ra.rel
    if hasattr(ra, 'inputs') and len(ra.inputs) > 0: return get_base_relation(ra.inputs[0])
    return None

def find_top_join(ra):
    if isinstance(ra, (radb.ast.Join, radb.ast.Cross)): return ra
    if hasattr(ra, 'inputs') and len(ra.inputs) == 1: return find_top_join(ra.inputs[0])
    return None

def get_subtree_relations(ra):
    rels = set()
    if isinstance(ra, radb.ast.RelRef): 
        rels.add(ra.rel)
    elif isinstance(ra, radb.ast.Rename): 
        rels.add(ra.relname)
    if hasattr(ra, 'inputs'):
        for child in ra.inputs: 
            rels.update(get_subtree_relations(child))
    return rels

def evaluate_pipeline(row, ra_node):
    if isinstance(ra_node, (radb.ast.RelRef, radb.ast.Join, radb.ast.Cross)): 
        return [row]
    if isinstance(ra_node, radb.ast.Select):
        input_rows = evaluate_pipeline(row, ra_node.inputs[0])
        return [r for r in input_rows if check_condition(r, ra_node.cond)]
    elif isinstance(ra_node, radb.ast.Rename):
        input_rows = evaluate_pipeline(row, ra_node.inputs[0])
        target = ra_node.relname
        result = []
        for r in input_rows:
            renamed = {}
            for k, v in r.items():
                col = k.split('.')[-1] if '.' in k else k
                renamed[f"{target}.{col}"] = v
            result.append(renamed)
        return result
    elif isinstance(ra_node, radb.ast.Project):
        input_rows = evaluate_pipeline(row, ra_node.inputs[0])
        output = []
        for r in input_rows:
            new_r = {}
            for attr in ra_node.attrs:
                val = parse_val(r, attr)
                if val is not None:
                    if attr.rel:
                        k = f"{attr.rel}.{attr.name}"
                    else:
                        k = attr.name
                        for existing_key in r.keys():
                            if existing_key.endswith("." + attr.name):
                                k = existing_key
                                break
                    new_r[k] = val
            if new_r:
                output.append(new_r)
        return output
    return [row]

def build_optimized_task(raquery, step, env):
    if isinstance(raquery, radb.ast.Project) and contains_join(raquery.inputs[0]):
        join_task = FoldedJoinTask(querystring=str(raquery.inputs[0]) + ";", step=step+1, exec_environment=env)
        return ProjectTask(querystring=str(raquery) + ";", step=step, exec_environment=env)
    elif contains_join(raquery):
        return FoldedJoinTask(querystring=str(raquery) + ";", step=step, exec_environment=env)
    else:
        return FoldedMapTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

class FoldedMapTask(RelAlgQueryTask):
    def requires(self):
        ra = radb.parse.one_statement_from_string(self.querystring)
        return [InputData(filename=f"{get_base_relation(ra)}.json", exec_environment=self.exec_environment)]
    
    def mapper(self, line):
        rel, val = line.split('\t', 1)
        for res in evaluate_pipeline(json.loads(val), radb.parse.one_statement_from_string(self.querystring)):
            yield rel, json.dumps(res)
    
    def reducer(self, key, values):
        ra = radb.parse.one_statement_from_string(self.querystring)
        has_projection = False
        current = ra
        while current:
            if isinstance(current, radb.ast.Project):
                has_projection = True
                break
            if hasattr(current, 'inputs') and len(current.inputs) > 0:
                current = current.inputs[0]
            else:
                break
        
        if has_projection:
            seen = set()
            for val in values:
                if val not in seen:
                    yield key, val
                    seen.add(val)
        else:
            for val in values:
                yield key, val

class FoldedJoinTask(RelAlgQueryTask):
    def requires(self):
        ra = radb.parse.one_statement_from_string(self.querystring)
        join = find_top_join(ra)
        reqs = []
        for i, child in enumerate(join.inputs):
            if contains_join(child):
                step_offset = 0 if i == 0 else count_steps(join.inputs[0])
                reqs.append(task_factory(child, step=self.step + 1 + step_offset, env=self.exec_environment, optimize=True))
            else:
                if isinstance(child, radb.ast.Select) or isinstance(child, radb.ast.Project) or isinstance(child, radb.ast.Rename):
                    reqs.append(FoldedMapTask(querystring=str(child) + ";", step=self.step + 1 + (0 if i == 0 else count_steps(join.inputs[0])), exec_environment=self.exec_environment))
                else:
                    reqs.append(InputData(filename=f"{get_base_relation(child)}.json", exec_environment=self.exec_environment))
        return reqs

    def mapper(self, line):
        rel, val = line.split('\t', 1)
        row = json.loads(val)
        ra = radb.parse.one_statement_from_string(self.querystring)
        join = find_top_join(ra)
        if isinstance(join, radb.ast.Cross):
            keys = []
        else:
            keys = get_join_attrs(join.cond)

        left_rels = get_subtree_relations(join.inputs[0])
        right_rels = get_subtree_relations(join.inputs[1])

        left_base = get_base_relation(join.inputs[0])
        right_base = get_base_relation(join.inputs[1])
        is_self_join = (left_base == right_base) and left_base is not None

        sides_to_process = []
        
        if is_self_join:
            row_relations = set()
            for k in row.keys():
                if '.' in k:
                    row_relations.add(k.split('.')[0])
            if row_relations & left_rels:
                sides_to_process = [('L', join.inputs[0])]
            elif row_relations & right_rels:
                sides_to_process = [('R', join.inputs[1])]
            else:
                sides_to_process = [('L', join.inputs[0]), ('R', join.inputs[1])]
        else:
            row_relations = set()
            for k in row.keys():
                if '.' in k:
                    row_relations.add(k.split('.')[0])
                else:
                    row_relations.add(k)
            
            has_left = bool(row_relations & left_rels)
            has_right = bool(row_relations & right_rels)
            
            if has_left or (not has_right and rel in left_rels):
                sides_to_process = [('L', join.inputs[0])]
            else:
                sides_to_process = [('R', join.inputs[1])]

        for tag, target_child in sides_to_process:
            processed = [row]
            if not contains_join(target_child):
                processed = evaluate_pipeline(row, target_child)

            for p_row in processed:
                key_vals = []
                valid = True
                for lk, rk in keys:
                    v = parse_val(p_row, lk)
                    if v is None: v = parse_val(p_row, rk)
                    if v is None: 
                        valid = False
                        break
                    key_vals.append(v)
                if valid or not keys:  
                    join_key = json.dumps(key_vals) if keys else "CROSS"
                    yield join_key, f"{tag}\t{rel}\t{json.dumps(p_row)}"

    def reducer(self, key, values):
        list_L, list_R = [], []
        for v in values:
            tag, rel, raw = v.split('\t', 2)
            (list_L if tag == 'L' else list_R).append(json.loads(raw))
        
        ra = radb.parse.one_statement_from_string(self.querystring)
        
        seen = set()
        for l in list_L:
            for r in list_R:
                m = l.copy()
                m.update(r)
                for res in evaluate_pipeline(m, ra):
                    res_str = json.dumps(res, sort_keys=True)
                    if res_str not in seen:
                        seen.add(res_str)
                        yield "JOIN_RESULT", res_str

class JoinTask(RelAlgQueryTask):
    def requires(self):
        ra = radb.parse.one_statement_from_string(self.querystring)
        return [task_factory(ra.inputs[0], step=self.step+1, env=self.exec_environment),
                task_factory(ra.inputs[1], step=self.step+count_steps(ra.inputs[0])+1, env=self.exec_environment)]
    def mapper(self, line):
        relation, tuple_str = line.split('\t', 1)
        row = json.loads(tuple_str)
        ra = radb.parse.one_statement_from_string(self.querystring)
        keys = get_join_attrs(ra.cond)
        left_rels = get_subtree_relations(ra.inputs[0])
        is_left = (relation in left_rels)
        if not is_left:
            for k in row.keys():
                if k.split('.')[0] in left_rels: is_left = True; break
        key_vals = []
        valid = True
        for lk, rk in keys:
             v = parse_val(row, lk)
             if v is None: v = parse_val(row, rk)
             if v is None: valid = False; break
             key_vals.append(v)
        if valid:
            tag = "L" if is_left else "R"
            yield json.dumps(key_vals), f"{tag}\t{relation}\t{tuple_str}"
    def reducer(self, key, values):
        list_L, list_R = [], []
        for v in values:
            tag, rel, raw = v.split('\t', 2)
            (list_L if tag == 'L' else list_R).append(json.loads(raw))
        for l in list_L:
            for r in list_R:
                m = l.copy(); m.update(r)
                yield "JOIN_RESULT", json.dumps(m)

class CrossTask(RelAlgQueryTask):
    def requires(self):
        ra = radb.parse.one_statement_from_string(self.querystring)
        return [task_factory(ra.inputs[0], step=self.step+1, env=self.exec_environment),
                task_factory(ra.inputs[1], step=self.step+count_steps(ra.inputs[0])+1, env=self.exec_environment)]
    def mapper(self, line):
        relation, tuple_str = line.split('\t', 1)
        row = json.loads(tuple_str)
        ra = radb.parse.one_statement_from_string(self.querystring)
        left_rels = get_subtree_relations(ra.inputs[0])
        is_left = (relation in left_rels)
        if not is_left:
            for k in row.keys():
                if k.split('.')[0] in left_rels: is_left = True; break
        tag = "L" if is_left else "R"
        yield "CROSS", f"{tag}\t{relation}\t{tuple_str}"
    def reducer(self, key, values):
        list_L, list_R = [], []
        for v in values:
            tag, rel, raw = v.split('\t', 2)
            (list_L if tag == 'L' else list_R).append(json.loads(raw))
        for l in list_L:
            for r in list_R:
                m = l.copy(); m.update(r)
                yield "CROSS_RESULT", json.dumps(m)

class SelectTask(RelAlgQueryTask):
    def requires(self):
        ra = radb.parse.one_statement_from_string(self.querystring)
        return [task_factory(ra.inputs[0], step=self.step+1, env=self.exec_environment)]
    def mapper(self, line):
        rel, tup = line.split('\t', 1)
        if check_condition(json.loads(tup), radb.parse.one_statement_from_string(self.querystring).cond):
            yield rel, tup

class RenameTask(RelAlgQueryTask):
    def requires(self):
        ra = radb.parse.one_statement_from_string(self.querystring)
        return [task_factory(ra.inputs[0], step=self.step+1, env=self.exec_environment)]
    def mapper(self, line):
        rel, tup = line.split('\t', 1)
        row = json.loads(tup)
        target = radb.parse.one_statement_from_string(self.querystring).relname
        yield target, json.dumps({f"{target}.{k.split('.')[-1] if '.' in k else k}": v for k,v in row.items()})

class ProjectTask(RelAlgQueryTask):
    def requires(self):
        ra = radb.parse.one_statement_from_string(self.querystring)
        return [task_factory(ra.inputs[0], step=self.step+1, env=self.exec_environment, optimize=True)]
    def mapper(self, line):
        rel, tup = line.split('\t', 1)
        row = json.loads(tup)
        attrs = radb.parse.one_statement_from_string(self.querystring).attrs
        new_tup = {}
        for attr in attrs:
            val = parse_val(row, attr)
            if val is not None:
                k = f"{attr.rel}.{attr.name}" if attr.rel else attr.name
                if not attr.rel:
                     for existing in row:
                         if existing.endswith("."+attr.name): k = existing; break
                new_tup[k] = val
        if new_tup:
            yield "PROJECT_RESULT", json.dumps(new_tup)
    def reducer(self, key, values):
        seen = set()
        for val in values:
            if val not in seen:
                yield key, val
                seen.add(val)