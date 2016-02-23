
/*
 * =====================================================================================
 *
 *       Filename:  wrapper_memcached.cpp
 *
 *    Description:  memcached wrapper 
 *
 *        Version:  1.0
 *        Created:  24/12/2015 11:00:29 AM
 *       Revision:  none
 *       Compiler:  g++
 *
 *         Author:  willrlzhang 
 *        Company:  
 *
 * =====================================================================================
 */

#include "wrapper_memcached.h"

bool Memcached::Init(const std::string& sSvrList)
{
	m_sSvrlist = sSvrList;	
	m_pMemc = memcached(sSvrList.c_str(), sSvrList.length());
	if (NULL == m_pMemc)
	{
		m_sErrMsg = "memcached failed."	;
		return false;
	}

	m_rc = memcached_behavior_set(m_pMemc, MEMCACHED_BEHAVIOR_DISTRIBUTION, MEMCACHED_DISTRIBUTION_CONSISTENT);
	if (MEMCACHED_SUCCESS != m_rc)
	{
		m_sErrMsg = "memcached_behavior_set failed.";
		return false;
	}

	return true;
}

Memcached::~Memcached()
{
	if (NULL != m_pMemc)
	{
		memcached_free(m_pMemc);
		m_pMemc = NULL;
	}
}

bool Memcached::Set(const std::string& sKey, const std::string& sValue, time_t tOverTime, uint32_t uFlags)
{
	if (m_pMemc == NULL)
	{
		m_sErrMsg = "set failed,  m_pMemc is NULL";
		return false;
	}

	m_rc = memcached_set(m_pMemc, sKey.c_str(), sKey.length(), sValue.c_str(), sValue.length(), tOverTime, uFlags);
	if (MEMCACHED_SUCCESS != m_rc)
	{
		m_sErrMsg = "memcached_set failed.";
		return false;
	}	

	return true;
}

const std::string  Memcached::GetErrMsg(void)
{
	std::string str = m_sErrMsg;
	if (m_pMemc != NULL)
	{
		const char* pMsg = memcached_last_error_message(m_pMemc);
		str = str + "\tErrMsg:" +  pMsg;
	}

	return str; 
}

bool Memcached::Delete(const std::string& sKey, time_t tOverTime)
{
	if (m_pMemc == NULL)
	{
		return false;
	}

	m_rc = memcached_delete(m_pMemc, sKey.c_str(), sKey.length(), tOverTime);
	if (MEMCACHED_SUCCESS != m_rc)
	{
		m_sErrMsg = "memcached_delete failed.";
		return false;
	}

	return true;
}

bool Memcached::CheckExist(std::string& sKey)
{
	if (m_pMemc == NULL)
	{
		return false;
	}

	m_rc = memcached_exist(m_pMemc, sKey.c_str(), sKey.length());
	if (m_rc != MEMCACHED_SUCCESS)
	{
		m_sErrMsg = "memchaced_exist failed.";
		return false;
	}

	return true;
}

std::string Memcached::Get(std::string& sKey, size_t& uValueLength, uint32_t& uFlags)
{
	if (m_pMemc == NULL)
	{
		return NULL;
	}
	
	char* pValue = memcached_get(m_pMemc, sKey.c_str(), sKey.length(), &uValueLength, &uFlags, &m_rc);

	return pValue;
}

