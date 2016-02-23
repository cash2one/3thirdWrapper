#include "wrapper_zkclient.h"
#include "comm_file_log.h"

CFileLog msg_log;
CFileLog err_log;

ZKClientMgr* ZKClientMgr::m_pInstance = NULL;
void WrapInitWatch(zhandle_t* zh, int type, int state, const char* path, void* watch)
{
    ZKClientMgr::GetInstance()->InitWatch(zh, type, state, path, watch);
}

void WrapServiceWatch(zhandle_t* zh, int type, int state, const char* path, void* watch)
{
    ZKClientMgr::GetInstance()->ServiceWatch(zh, type, state, path, watch);
}

void WrapCreateWatch(int rc, const char* name, const void* data)
{
        ZKClientMgr::GetInstance()->CreateWatch(rc, name, data);
}

void WrapDeleteWatch(int rc, const void* data)
{
        ZKClientMgr::GetInstance()->DeleteWatch(rc, data);
}

bool ZKClient::init(const std::string& strAddrs,
    const ZooLogLevel& logLevel,int timeout) 
{
    if (m_zh == NULL)
    {
        zoo_set_debug_level(logLevel);
        m_zh = zookeeper_init(strAddrs.c_str(), 
                        WrapInitWatch, timeout, 0,
                        const_cast<char*>("TODO"), 0);
    }

    return true;
}

void ZKClient::finalize()
{
    if (m_zh != NULL)
    {
        zookeeper_close(m_zh);
        m_zh = NULL;
    }
}

bool ZKClient::registWatchChildren(const std::string& strPath, strings_completion_t cbfun)
{
    if (m_zh == NULL)
    {
        err_log.Write("<ZKClient::registWatchChildren> invalid handle\n");
        return false;
    }

    int iRet = zoo_awget_children(m_zh, strPath.c_str(), WrapServiceWatch, NULL, cbfun, NULL);
    if (iRet != ZOK)
    {
        err_log.Write("<ZKClient::registWatchChildren> async get regist failed, result: %d\n", iRet);
        return false;
    }
    m_watches[strPath] = cbfun;
    return true;
}

bool ZKClient::acreat(const std::string& strPath)
{
    if (m_zh == NULL)
    {
        err_log.Write("<ZKClient::acreate> invalid handle\n");
        return false;
    }
    int iRet = zoo_acreate(m_zh, strPath.c_str(), "", 8, &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, WrapCreateWatch, "create");

    if (iRet != ZOK)
    {
        err_log.Write("<ZKClient::acreate> create error %d\n", iRet);
        return false;
    }
    return true;

}

bool ZKClient::adelete(const std::string& strPath)
{
    if (m_zh == NULL)
    {
        err_log.Write("<ZKClient::adelete> invalid handle\n");
        return false;
    }

    int iRet = zoo_adelete(m_zh, strPath.c_str(), -1, WrapDeleteWatch, "delete");
    if (iRet != ZOK)
    {
        err_log.Write("<ZKClient::adelete> delete error %d\n", iRet);
        return false;
    }
    return true;
}

void ZKClient::InitWatch(zhandle_t* zh, int type, int state, const char* path, void* watch)
{
    if (type == ZOO_SESSION_EVENT)
    {
        if (state == ZOO_CONNECTED_STATE)
        {
            msg_log.Write("<ZKClient::InitWatch> build connection ok, timeout: %d\n", zoo_recv_timeout(zh));
        }
        else if (state == ZOO_EXPIRED_SESSION_STATE)
        {
            err_log.Write("<ZKClient::InitWatch> connection disconnect\n");
        }
        else if (state == ZOO_CONNECTING_STATE)
        {
            msg_log.Write("<ZKClient::InitWatch> connecting\n");
        }
    }
}

void ZKClient::ServiceWatch(zhandle_t* zh, int type, int state, const char* path, void* watch)
{
    if (ZOO_CHILD_EVENT == type)
    {
        msg_log.Write("<ZKClient::ServiceWatch> watch child event\n");
        std::string strPath(path);
        if (m_watches.find(strPath) == m_watches.end())
        {
            err_log.Write("<ZKClient::ServiceWatch> watch function not found %s\n", strPath.c_str());
            return;
        }
        registWatchChildren(std::string(path), m_watches[path]);
    }
    else
    {
        msg_log.Write("<ZKClient::ServiceWatch> watch event: %d\n", type);
    }
}

void ZKClient::CreateWatch(int rc, const char* name, const void* data)
{
    if (rc == ZNODEEXISTS || rc == ZOK)
    {
        if (rc == ZOK)
        {
            msg_log.Write("<ZKClient::CreateWatch>registry ok\n");
        }
        else
        {
            msg_log.Write("<ZKClient::CreateWatch>node exist\n");
        }
    }
    else
    {
        err_log.Write("<ZKClient::CreateWatch>registry error: %d\n", rc);
    }
}

void ZKClient::DeleteWatch(int rc, const void* data)
{
    if (rc == ZOK)
    {
        msg_log.Write("<ZKClient::DeleteWatch>registry ok\n");
    }
    else
    {
        err_log.Write("<ZKClient::DeleteWatch>registry error: %d\n", rc);
    }
}

