/* This is a generated file, edit the .stub.php file instead.
 * Stub hash: 0300ebbc284335b34bfde6d170aeae4a8455a88a */

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_hazedb_fetch, 0, 1, IS_ARRAY, 1)
	ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, args, IS_MIXED, 0, "null")
ZEND_END_ARG_INFO()

#define arginfo_hazedb_fetchall arginfo_hazedb_fetch

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_hazedb_fetchall_json, 0, 1, IS_STRING, 1)
	ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, args, IS_MIXED, 0, "null")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_hazedb_exec, 0, 1, IS_LONG, 0)
	ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, args, IS_MIXED, 0, "null")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_hazedb_ping, 0, 0, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_FUNCTION(hazedb_fetch);
ZEND_FUNCTION(hazedb_fetchall);
ZEND_FUNCTION(hazedb_fetchall_json);
ZEND_FUNCTION(hazedb_exec);
ZEND_FUNCTION(hazedb_ping);

static const zend_function_entry ext_functions[] = {
	ZEND_FE(hazedb_fetch, arginfo_hazedb_fetch)
	ZEND_FE(hazedb_fetchall, arginfo_hazedb_fetchall)
	ZEND_FE(hazedb_fetchall_json, arginfo_hazedb_fetchall_json)
	ZEND_FE(hazedb_exec, arginfo_hazedb_exec)
	ZEND_FE(hazedb_ping, arginfo_hazedb_ping)
	ZEND_FE_END
};
