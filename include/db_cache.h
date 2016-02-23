/*
 * =====================================================================================
 *
 *       Filename:  db_cache.h
 *
 *    Description:  将数据库信息cache到内存中
 *
 *        Version:  1.0
 *        Created:  24/11/2015 11:00:29 AM
 *       Revision:  none
 *       Compiler:  g++
 *
 *         Author:  willrlzhang 
 *        Company:  Tencent
 *
 * =====================================================================================
 */
#ifndef __DB_CACHE_H_20151124_willrlzhang__
#define __DB_CACHE_H_20151124_willrlzhang__

#include "comm_utils.h"
#include "wrapper_mysql2.h"
#include <string>
#include <vector>
#include <map>
#include <tr1/unordered_map>

class CDbCache
{
public:
	typedef std::vector<std::string> SQL_RECORD;
	typedef std::vector<std::vector<std::string> > SQL_RESULT;

public:
	CDbCache(void);

	// 初始化(数据库信息)
	bool Init(const std::string& sDbHost, const std::string& sDbName,
			const std::string& sDbUser, const std::string& sDbPass,
			const std::string& sCharsetName = "utf8", const uint16_t dwDbPort=0);

	// 重新设置数据库信息
	bool SetDatabase(const std::string& sDbHost, const std::string& sDbName,
			const std::string& sDbUser, const std::string& sDbPass,
			const std::string& sCharsetName = "utf8", const uint16_t dwDbPort=0);

	// 执行sql语句
	bool ExecSql(const std::string& sSql, int32_t iExpectFieldNum = 0);

	SQL_RESULT SqlResult() {return m_stSqlRes;};
	// 执行sql返回的字段数量
	int32_t GetFieldCount() 
	{
		if ( !m_bExecSql )
		{
			m_sErrMsg = "Get field number failed because no sql was executed! ";
			return -1;
		}
		return m_iFieldNum;
	}

	// 执行sql返回结果的行数
	int32_t GetRowCount() 
	{
		if ( !m_bExecSql )
		{
			m_sErrMsg = "Get row number failed because no sql was executed!";
		}
		return m_iRowNum;
	}

	static std::string MysqlEscape(const std::string &str)
	{
		return dt::CMysql::MysqlEscape(str);
	}

	static std::string MysqlEscape(const char* pData, const int iSize)
	{
		return dt::CMysql::MysqlEscape(pData, iSize);
	}

	// load到vector中(支持 std::vector | std::deque)
	template<class T, template<class t> class VEC>
	bool Load2Vec(VEC<T>& vecCache, void* pUserInfo = NULL)
	{
		if ( !m_bExecSql )
		{
			m_sErrMsg = "Load2Vec failed because no sql was executed! ";
			return false;
		}

		vecCache.clear();
		for ( uint32_t i = 0; i < m_stSqlRes.size(); i++ )
		{
			T elem;
			if ( elem.SetValue(m_stSqlRes[i], pUserInfo) ) /* 如果SetValue返回false, 就忽略此节点 */
			{
				vecCache.push_back(elem);
			}
		}

		return true;
	}

	// load到set中(支持 std::set | std::tr1::unordered_set)
	template <class T, class O, template<class t, class o> class SET>
	bool Load2Set(SET<T,O>& setCache, void* pUserInfo = NULL)
	{
		if ( !m_bExecSql )
		{
			m_sErrMsg = "Load2Vec failed because no sql was executed! ";
			return false;
		}

		setCache.clear();
		for ( uint32_t i = 0; i < m_stSqlRes.size(); i++ )
		{
			T elem;
			if ( elem.SetValue(m_stSqlRes[i], pUserInfo) ) /* 如果SetValue返回false, 就忽略此节点 */
			{
				setCache.insert(elem);
			}
		}

		return true;
	}

	// load到map中(支持 std::map | std::multimap | std::tr1::unorder_map)
	template<class K, class T, class O, template<class k, class t, class o> class MAP>
	bool Load2Map(MAP<K,T,O>& mapCache, K* pUserInfo = NULL)
	{
		if ( !m_bExecSql )
		{
			m_sErrMsg = "Load2Map failed because no sql was executed! ";
			return false;
		}

		mapCache.clear();
		for ( uint32_t i = 0; i < m_stSqlRes.size(); i++ )
		{
			T elem;
			if ( elem.SetValue(m_stSqlRes[i], pUserInfo) ) /* 如果SetValue返回false，就忽略此节点 */
			{
				K key = elem.key(m_stSqlRes[i], pUserInfo);
				mapCache.insert( make_pair(key, elem) );
			}
		}

		return true;
	}

	// 偏特化 MAP<K,vector<T>> 的情况
	template <class K, class T, class O, template<class k, class t, class o> class MAP>
	bool Load2Map(MAP<K,std::vector<T>,O>& mapCache, K* pUserInfo = NULL)
	{
		if ( !m_bExecSql )
		{
			m_sErrMsg = "Load2Map failed because no sql was executed! ";
			return false;
		}

		mapCache.clear();
		for ( uint32_t i = 0; i < m_stSqlRes.size(); i++ )
		{
			T elem;
			if ( elem.SetValue(m_stSqlRes[i], pUserInfo) ) /* 如果SetValue返回false，就忽略此节点 */
			{
				K key = elem.key(m_stSqlRes[i], pUserInfo);
				mapCache[key].push_back(elem);
			}
		}

		return true;
	}

	// 定义函数模板
	template<class K, class T, class O, template<class k, class t, class o> class MAP>
	static bool GetMapElem(const MAP<K,T,O>& mapCache, const K& key, T& value)
	{
		typename MAP<K,T,O>::const_iterator itor = mapCache.find(key);
		if ( itor != mapCache.end() )
		{
			value = itor->second;
			return true;
		}

		return false;
	}

	// 偏特化 multimap 的情况
	template<class K, class T, class O>
	static bool GetMapElem(const std::multimap<K,T,O>& multimapCache, const K& key, std::vector<T>& vecValue)
	{
		typedef typename std::multimap<K,T,O>::const_iterator CONST_MULTIMAP_ITERATOR;

		std::pair<CONST_MULTIMAP_ITERATOR, CONST_MULTIMAP_ITERATOR> pair_const_itor;
		pair_const_itor = multimapCache.equal_range(key);

		if ( pair_const_itor.first == pair_const_itor.second ) /* not found */
		{
			return false;
		}

		vecValue.clear();
		transform(pair_const_itor.first, pair_const_itor.second, back_inserter(vecValue), CPairSecondExtractor());

		return true;
	}

	const std::string GetErrMsg(void) const
	{
		return m_sErrMsg;
	}

private:
	dt::CMysql m_oMysql;

	bool m_bInit;                               /* 是否已经初始化 */
	bool m_bExecSql;                            /* 是否已经执行过sql语句 */

	int32_t m_iFieldNum;
	int32_t m_iRowNum;

	SQL_RESULT m_stSqlRes;                      /* sql处理后的结果集(vector<string>的形式存放) */
	std::string m_sErrMsg;
};

#endif
