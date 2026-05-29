/* This is a generated file, edit the .stub.php file instead.
 * Stub hash: 2a80cd56e808e4e9abb1f758defd4e0ff78769e9 */

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_hazedb_query, 0, 2, IS_STRING, 1)
	ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, args_json, IS_STRING, 0)
ZEND_END_ARG_INFO()

#define arginfo_hazedb_exec arginfo_hazedb_query

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_hazedb_uuidv7, 0, 0, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_FUNCTION(hazedb_query);
ZEND_FUNCTION(hazedb_exec);
ZEND_FUNCTION(hazedb_uuidv7);

static const zend_function_entry ext_functions[] = {
	ZEND_FE(hazedb_query, arginfo_hazedb_query)
	ZEND_FE(hazedb_exec, arginfo_hazedb_exec)
	ZEND_FE(hazedb_uuidv7, arginfo_hazedb_uuidv7)
	ZEND_FE_END
};
