/* This is a generated file, edit the .stub.php file instead.
 * Stub hash: 8f1148f41723df748d095997036f5f38fa5243fc */

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_hazedb_query, 0, 2, IS_STRING, 1)
	ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, args, IS_STRING, 0)
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
