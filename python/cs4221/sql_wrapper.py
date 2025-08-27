# /opt/app/bin/sql_wrapper.py
import os, re, sys

from pyspark.sql import SparkSession

def subst_vars(sql_text: str) -> str:
    """Replace ${VAR} with os.environ['VAR'] (or empty if not set),
       and ${env:VAR} explicitly from env."""
    def repl(m):
        key = m.group(1)
        if key.startswith("env:"):
            return os.environ.get(key[4:], "")
        return os.environ.get(key, os.environ.get(key, ""))
    return re.sub(r"\$\{([^}]+)\}", repl, sql_text)

def split_statements(sql_text: str):
    """Split on semicolons not inside quotes."""
    stmts, buf, sgl, dbl, esc = [], [], False, False, False
    for ch in sql_text:
        if ch == "\\" and not esc:
            esc = True
            buf.append(ch)
            continue
        if ch == "'" and not dbl and not esc:
            sgl = not sgl
        elif ch == '"' and not sgl and not esc:
            dbl = not dbl
        if ch == ";" and not sgl and not dbl:
            st = "".join(buf).strip()
            if st:
                stmts.append(st)
            buf = []
        else:
            buf.append(ch)
        esc = False
    tail = "".join(buf).strip()
    if tail:
        stmts.append(tail)
    return [s for s in stmts if s]

def main():
    sql_path = os.environ.get("SQL_PATH", "/opt/app/sql/w1_step2.sql")

    spark = (
        SparkSession.builder
        .appName("SQL-Wrapper")
        .enableHiveSupport()
        .getOrCreate()
    )

    with open(sql_path, "r") as f:
        raw = f.read()

    expanded = subst_vars(raw)
    statements = split_statements(expanded)

    print(f"[sql_wrapper] executing {len(statements)} statement(s) from {sql_path}")
    for i, st in enumerate(statements, 1):
        print(f"\n--[sql_wrapper] {i}/{len(statements)} --\n{st}\n")
        spark.sql(st)

    print("[sql_wrapper] done.")

if __name__ == "__main__":
    main()
