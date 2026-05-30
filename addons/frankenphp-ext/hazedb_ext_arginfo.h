/* This is a generated file, edit the .stub.php file instead.
 * Stub hash: f4403f03eaae60d18742120e2b61e4dbef2fbadf */

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_hazedb_query, 0, 2, IS_STRING, 1)
	ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, args, IS_STRING, 0)
ZEND_END_ARG_INFO()

#define arginfo_hazedb_exec arginfo_hazedb_query

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_hazedb_ping, 0, 0, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_FUNCTION(hazedb_query);
ZEND_FUNCTION(hazedb_exec);
ZEND_FUNCTION(hazedb_ping);

static const zend_function_entry ext_functions[] = {
	ZEND_FE(hazedb_query, arginfo_hazedb_query)
	ZEND_FE(hazedb_exec, arginfo_hazedb_exec)
	ZEND_FE(hazedb_ping, arginfo_hazedb_ping)
	ZEND_FE_END
};
