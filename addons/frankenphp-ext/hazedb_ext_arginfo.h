/* This is a generated file, edit the .stub.php file instead.
 * Stub hash: 03933dc045a7b6dc2d7b0811f489852405463b6f */

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_hazedb_query, 0, 2, IS_STRING, 1)
	ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, args, IS_STRING, 0)
ZEND_END_ARG_INFO()

#define arginfo_hazedb_exec arginfo_hazedb_query

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_hazedb_query_arr, 0, 2, IS_ARRAY, 1)
	ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, args, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_hazedb_get, 0, 2, IS_ARRAY, 1)
	ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, id, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_hazedb_exec_arr, 0, 2, IS_STRING, 1)
	ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, args, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_hazedb_ping, 0, 0, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_FUNCTION(hazedb_query);
ZEND_FUNCTION(hazedb_exec);
ZEND_FUNCTION(hazedb_query_arr);
ZEND_FUNCTION(hazedb_get);
ZEND_FUNCTION(hazedb_exec_arr);
ZEND_FUNCTION(hazedb_ping);

static const zend_function_entry ext_functions[] = {
	ZEND_FE(hazedb_query, arginfo_hazedb_query)
	ZEND_FE(hazedb_exec, arginfo_hazedb_exec)
	ZEND_FE(hazedb_query_arr, arginfo_hazedb_query_arr)
	ZEND_FE(hazedb_get, arginfo_hazedb_get)
	ZEND_FE(hazedb_exec_arr, arginfo_hazedb_exec_arr)
	ZEND_FE(hazedb_ping, arginfo_hazedb_ping)
	ZEND_FE_END
};
