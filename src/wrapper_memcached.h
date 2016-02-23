/*
 * =====================================================================================
 *
 *       Filename:  wrapper_memcached.h
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

#ifndef __MEMCACHED_H_20151224_willrlzhang__
#define __MEMCACHED_H_20151224_willrlzhang__

#include <string> 
#include "libmemcached/memcached.h"

class Memcached
{
	public:
		//sSvrList format --SERVER=ip1:port,ip2:port,ip3:port
		bool Init(const std::string& sSvrList);
		Memcached(){};
		~Memcached();

		bool Set(const std::string& sKey, const std::string& sValue, time_t tOverTime, uint32_t uFlags);
		const std::string GetErrMsg(void);
		bool Delete(const std::string& sKey, time_t tOverTime);
		bool CheckExist(std::string& sKey);
		std::string Get(std::string& sKey, size_t& uValueLength, uint32_t& uFlag);

	private:
		memcached_st* m_pMemc;
		memcached_server_st*  m_pServers;
		memcached_return m_rc;

		std::string m_sErrMsg;
		std::string m_sSvrlist;
};

class MemcachedMgr:public Memcached
{
	public:
		~MemcachedMgr()
		{}

		static MemcachedMgr* GetInstance()
		{
			if (m_pInstance == NULL)
			{
				m_pInstance = new MemcachedMgr;
			}

			return m_pInstance;
		}

	private:
		MemcachedMgr()
		{
		}

		static MemcachedMgr* m_pInstance;

};

#endif
