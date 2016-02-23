#include "wrapper_mysql2.h"
#include <stdio.h>
#include <string.h>

namespace dt{

	CMysql::CMysql(void)
		:m_pszNULL("")
	{
		m_bConnected=false;
		m_pRes=NULL;
		m_pField = NULL;
		m_bQuery = false;
		m_sCharsetName = "utf8";
	}

	CMysql::CMysql(const std::string& sHost,const std::string& sDBName,const std::string& sUser,const std::string& sPasswd, const uint16_t wDbPort)
		:m_sHost(sHost)
		,m_sDBName(sDBName)
		,m_sUserName(sUser)
		,m_sPasswd(sPasswd)
		,m_wDbPort(wDbPort)
		,m_pszNULL("")
	{
		m_pRes = NULL;
		m_pField = NULL;
		m_bConnected = false;
		m_bQuery = false;
		m_sCharsetName = "utf8";
	}

	CMysql::~CMysql(void)
	{
		if(m_pRes!=NULL)
		{
			mysql_free_result(m_pRes);
			m_pRes = NULL;
		}

		if(m_bConnected)
		{
			mysql_close(&m_stMySql);
		}
	}

	bool CMysql::SetDatabase(const std::string& sHost, const std::string& sDBName, const std::string& sUser, const std::string& sPasswd, const uint16_t wDbPort)
	{
		if(m_bConnected)
		{
			Disconnect();
		}
		m_sHost=sHost;
		m_sDBName=sDBName;
		m_sUserName=sUser;
		m_sPasswd=sPasswd;
		m_wDbPort = wDbPort;

		return true;
	}

	bool CMysql::Init(const std::string& sHost, const std::string& sDBName, const std::string& sUser, const std::string& sPasswd, const uint16_t wDbPort)
	{
		return SetDatabase(sHost, sDBName, sUser, sPasswd, wDbPort);
	}

	bool CMysql::Connect()
	{
		m_bQuery = false;

		mysql_init(&m_stMySql);
		mysql_options(&m_stMySql, MYSQL_SET_CHARSET_NAME, m_sCharsetName.c_str());
		MYSQL *pMySql=mysql_real_connect(&m_stMySql, m_sHost.c_str(),m_sUserName.c_str(),m_sPasswd.c_str(),m_sDBName.c_str(),m_wDbPort,NULL,0) ;
		if (pMySql== NULL)
		{
			snprintf(m_szErrMsg,sizeof(m_szErrMsg),"%s", mysql_error(&m_stMySql));
			m_uiErrCode=mysql_errno(&m_stMySql);
			return false;
		}
		return true;
	}

	void CMysql::Disconnect()
	{
		if(m_pRes!=NULL)
		{
			mysql_free_result(m_pRes);
			m_pRes=NULL;
		}
		if(m_bConnected)mysql_close(&m_stMySql);
		m_bConnected=false;
		m_bQuery = false;
	}

	bool CMysql::Query(const std::string& sSql)
	{
		if(!m_bConnected)
		{
			if(!Connect())
			{
				snprintf(m_szErrMsg,sizeof(m_szErrMsg),"%s", mysql_error(&m_stMySql));
				m_uiErrCode = mysql_errno(&m_stMySql);
				return false;
			}
			m_bConnected=true;
		}
		if(m_pRes!=NULL)
		{
			mysql_free_result(m_pRes);
			m_pRes = NULL;
		}
		if(mysql_real_query(&m_stMySql,sSql.c_str(),static_cast<uint32_t>(strlen(sSql.c_str())))!=0)
		{
			snprintf(m_szErrMsg,sizeof(m_szErrMsg),"%s", mysql_error(&m_stMySql));
			m_uiErrCode=mysql_errno(&m_stMySql);
			return false;
		}
		m_pRes=mysql_store_result(&m_stMySql);

		m_dwAffectedRows = static_cast<uint32_t>(mysql_affected_rows(&m_stMySql)); 

		if(m_pRes==NULL)
		{
			m_dwRowCount=0;
			m_uiFieldCount=0;
		}
		else 
		{
			m_dwRowCount = static_cast<uint32_t>(mysql_num_rows(m_pRes));
			m_uiFieldCount = mysql_num_fields(m_pRes);

			m_pField = mysql_fetch_fields(m_pRes); /* 获取field的信息(add by sunnyhao) */
		}

		m_bQuery = true;
		return true;
	}


	bool CMysql::Next()
	{
		bool bOk = false;
		if(m_pRes!=NULL && m_dwRowCount>0)
		{
			m_stRow=mysql_fetch_row(m_pRes);
			if(m_stRow==NULL)
			{
				snprintf(m_szErrMsg,sizeof(m_szErrMsg),"%s", mysql_error(&m_stMySql));
				m_uiErrCode=mysql_errno(&m_stMySql);
				bOk = false;
			}
			else
			{
				bOk = true;
			}
		}
		return bOk;
	}

	const char* CMysql::GetFieldName(const uint32_t index)
	{
		if ( m_pField != NULL && index < m_uiFieldCount )
		{
			return m_pField[index].name;
		}
		return NULL;
	}

	const char* CMysql::GetRow(uint32_t index)
	{
		if ( index < m_uiFieldCount)
		{
			if (NULL == m_stRow[index])
			{
				return m_pszNULL;
			}
			else
			{
				return m_stRow[index];
			}
		}
		else 
		{
			return m_pszNULL;
		}

		return m_pszNULL;
	}

	const char* CMysql::GetRecord(const uint32_t index)
	{
		if ( index < m_uiFieldCount)
		{
			if (NULL == m_stRow[index])
			{
				return m_pszNULL;
			}
			else
			{
				return m_stRow[index];
			}
		}
		else 
		{
			return m_pszNULL;
		}

		return m_pszNULL;
	}

};

