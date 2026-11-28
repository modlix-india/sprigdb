-- Runs one of sysbench's bundled OLTP tests with every connection's
-- transaction isolation set first (sysbench has no option for it), and
-- prints the level the server reports on the first thread's connection, so
-- sql/sysbench.sh can check the server didn't quietly use another one.
--
--   SB_TEST=oltp_read_write SB_ISOLATION_SQL="SET SESSION ..." \
--   SB_ISOLATION_CHECK="SELECT @@transaction_isolation" sysbench sql/isolation.lua ... run

local test = os.getenv("SB_TEST") or error("SB_TEST is not set")
require(test)

local bundled_init = thread_init

function thread_init(thread_id)
  bundled_init(thread_id)
  con:query(os.getenv("SB_ISOLATION_SQL") or error("SB_ISOLATION_SQL is not set"))
  if thread_id == 0 then
    local rs = con:query(os.getenv("SB_ISOLATION_CHECK") or error("SB_ISOLATION_CHECK is not set"))
    local row = rs:fetch_row()
    print("isolation in effect: " .. tostring(row[1]))
  end
end
