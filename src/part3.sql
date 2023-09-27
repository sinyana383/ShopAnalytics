CREATE ROLE Administrator;

GRANT pg_read_all_data TO Administrator;
GRANT pg_write_all_data TO Administrator;
GRANT pg_monitor TO Administrator;
GRANT pg_signal_backend TO Administrator;
GRANT pg_read_server_files TO Administrator;
GRANT pg_write_server_files TO Administrator;
GRANT pg_execute_server_program TO Administrator;

CREATE ROLE Visitor;
GRANT pg_read_all_data TO Visitor;

-- посмотреть все роли
SELECT rolname FROM pg_roles;