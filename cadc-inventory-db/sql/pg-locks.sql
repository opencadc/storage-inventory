-- examine current locks
select locktype,relation,C.relname,page,tuple,virtualxid,virtualtransaction,mode,granted
from pg_locks L join pg_class C on L.relation = C.oid 
where relname not like 'pg_%';

