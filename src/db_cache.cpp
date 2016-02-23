/*
 * =====================================================================================
 *
 *       Filename:  db_cache.cpp
 *
 *    Description:  将数据库信息cache到内存中
 *
 *        Version:  1.0
 *        Created:  24/11/2015 10:59:32 AM
 *       Revision:  none
 *       Compiler:  g++
 *
 *         Author:  willrlzhang 
 *        Company:  Tencent
 *
 * =====================================================================================
 */
#include "db_cache.h"

CDbCache::CDbCache(): m_bInit(false), m_bExecSql(false), m_iFieldNum(0), m_iRowNum(0), m_sErrMsg("")
{}

bool CDbCache::Init(const std::string& sDbHost, const std::string& sDbName,
		const std::string& sDbUser, const std::string& sDbPass, 
		const std::string& sCharsetName, const uint16_t dwDbPort)
{
	if ( m_bInit )
	{
		m_sErrMsg = "Init failed because databases has been initialized before!";
		return false;
	}

	bool bRet = m_oMysql.Init(sDbHost, sDbName, sDbUser, sDbPass, dwDbPort);
	if ( !bRet ) 
	{
		m_sErrMsg = std::string("Mysql initialize failed! Msg: ") + m_oMysql.GetErrMsg();
		return false;
	}

	m_oMysql.SetCharsetName(sCharsetName);
	m_bInit = true;

	return true;
}

bool CDbCache::SetDatabase(const std::string& sDbHost, const std::string& sDbName,
		const std::string& sDbUser, const std::string& sDbPass,
		const std::string& sCharsetName, const uint16_t dwDbPort)
{
	bool bRet = m_oMysql.SetDatabase(sDbHost, sDbName, sDbUser, sDbPass, dwDbPort);
	if ( !bRet )
	{
		m_sErrMsg = std::string("Mysql set databases failed! Msg: ") + m_oMysql.GetErrMsg();
		return false;
	}

	m_oMysql.SetCharsetName(sCharsetName);

	m_bInit = true;
	return true;
}

bool CDbCache::ExecSql(const std::string& sSql, int32_t iExpectFieldNum)
{
	if ( !m_bInit )
	{
		m_sErrMsg = "CDbCache has not been initialized!";
		return false;
	}

	m_bExecSql = false;                         /* 复原状态 */
	m_stSqlRes.clear();

	if ( !m_oMysql.Query(sSql) )
	{
		m_sErrMsg = std::string("Mysql query failed! Msg: ") + m_oMysql.GetErrMsg();
		return false;
	}
	else
	{
		m_iRowNum = m_oMysql.GetRowCount();
		for ( int32_t i = 0; i < m_iRowNum; ++i ) 
		{
			if ( !m_oMysql.Next() )
			{
				m_sErrMsg = std::string("Mysql next failed! Msg: ") + m_oMysql.GetErrMsg();
				return false;
			}
			else
			{
				// 这里检查保证构造函数中不会非法引用
				m_iFieldNum = m_oMysql.GetFieldCount();
				if ( iExpectFieldNum > m_iFieldNum )
				{
					m_sErrMsg = "Field number of mysql result is " + ToStr(m_iFieldNum) + " less than expected!";
					return false;
				}

				SQL_RECORD record;
				for ( uint32_t j = 0; j < m_oMysql.GetFieldCount(); j++ )
				{
					record.push_back( m_oMysql.GetRow(j) );
				}

				m_stSqlRes.push_back(record);
			}
		}
	}

	m_bExecSql = true;
	return true;
}
