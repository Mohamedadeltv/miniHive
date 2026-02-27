import subprocess
import re

# The list of queries from miniHive.q
queries = [
    "select distinct C_NAME, C_ADDRESS from CUSTOMER where C_CUSTKEY=42",
    "select distinct C.C_NAME, C.C_ADDRESS from CUSTOMER C where C.C_NATIONKEY=7",
    "select distinct * from CUSTOMER, NATION where CUSTOMER.C_NATIONKEY=NATION.N_NATIONKEY and NATION.N_NAME='GERMANY'",
    "select distinct CUSTOMER.C_CUSTKEY from CUSTOMER, NATION where CUSTOMER.C_NATIONKEY=NATION.N_NATIONKEY and NATION.N_NAME='GERMANY'",
    "select distinct CUSTOMER.C_CUSTKEY from CUSTOMER, NATION where CUSTOMER.C_NATIONKEY=NATION.N_NATIONKEY and CUSTOMER.C_CUSTKEY=42",
    "select distinct CUSTOMER.C_CUSTKEY from CUSTOMER, NATION, REGION where CUSTOMER.C_NATIONKEY=NATION.N_NATIONKEY and NATION.N_REGIONKEY = REGION.R_REGIONKEY",
    "select distinct CUSTOMER.C_CUSTKEY from REGION, NATION, CUSTOMER where CUSTOMER.C_NATIONKEY=NATION.N_NATIONKEY and NATION.N_REGIONKEY = REGION.R_REGIONKEY",
    "select distinct * from ORDERS, CUSTOMER where ORDERS.O_ORDERPRIORITY='1-URGENT' and CUSTOMER.C_CUSTKEY=ORDERS.O_CUSTKEY",
    "select distinct * from CUSTOMER, ORDERS, LINEITEM where CUSTOMER.C_CUSTKEY=ORDERS.O_CUSTKEY and ORDERS.O_ORDERKEY = LINEITEM.L_ORDERKEY and LINEITEM.L_SHIPMODE='AIR' and CUSTOMER.C_MKTSEGMENT = 'HOUSEHOLD'",
    "select distinct * from LINEITEM,ORDERS,CUSTOMER where CUSTOMER.C_CUSTKEY=ORDERS.O_CUSTKEY and ORDERS.O_ORDERKEY = LINEITEM.L_ORDERKEY and LINEITEM.L_SHIPMODE='AIR' and CUSTOMER.C_MKTSEGMENT = 'HOUSEHOLD'"
]

def get_cost(command):
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        lines = result.stdout.strip().split('\n')
        
        # FIX: Iterate backwards to find the first valid integer
        # This ignores "Loading GCP..." warnings at the end
        for line in reversed(lines):
            clean_line = line.strip()
            if clean_line.isdigit():
                return int(clean_line)
        return -1
    except Exception as e:
        return -1

print(f"{'#':<3} | {'Unoptimized':<12} | {'Optimized':<10} | {'Reduction':<10} | {'Status'}")
print("-" * 60)

for i, q in enumerate(queries):
    # 1. Run Unoptimized
    cmd_base = f'python3 miniHive.py --env LOCAL "{q}"'
    cost_base = get_cost(cmd_base)

    # 2. Run Optimized
    cmd_opt = f'python3 miniHive.py --O --env LOCAL "{q}"'
    cost_opt = get_cost(cmd_opt)

    # 3. Calculate Stats
    reduction = 0.0
    status = "FAIL"
    
    if cost_base > 0:
        reduction = (1 - (cost_opt / cost_base)) * 100
    
    if cost_opt == -1 or cost_base == -1:
        status = "ERROR ⚠️"
    elif cost_opt <= cost_base:
        status = "PASS ✅"
        if reduction > 66.0:
            status = "GREAT 🌟"
    else:
        status = "FAIL ❌"

    print(f"{i+1:<3} | {cost_base:<12} | {cost_opt:<10} | {reduction:<9.1f}% | {status}")