#ifndef __ZKCLIENT_H
#define __ZKCLIENT_H
#include <string>
#include <map>
#include <zookeeper/zookeeper.h>

void InitWatch(zhandle_t* zh, int type, int state, const char* path, void* watch);
void ServiceWatch(zhandle_t* zh, int type, int state, const char* path, void* watch);
void CreateWatch(int rc, const char* name, const void* data);
void DeleteWatch(int rc, const void* data);

class ZKClient
{
    public:
        ZKClient(){};

        ~ZKClient(){};

        bool init(const std::string& strAddrs, const ZooLogLevel& logLevel=ZOO_LOG_LEVEL_ERROR,int timeout=1000);
        void finalize();
        bool registWatchChildren(const std::string& strPath, strings_completion_t cbfun);
        bool acreat(const std::string& strPath);
        bool adelete(const std::string& strPath);

        void InitWatch(zhandle_t* zh, int type, int state, const char* path, void* watch);
        void ServiceWatch(zhandle_t* zh, int type, int state, const char* path, void* watch);
        void CreateWatch(int rc, const char* name, const void* data);
        void DeleteWatch(int rc, const void* data);

    private:
        zhandle_t* m_zh;
        std::map<std::string, strings_completion_t> m_watches;
};

class ZKClientMgr: public ZKClient
{
    public:
            ~ZKClientMgr();
            static ZKClientMgr* GetInstance(void)
            {
                    if (m_pInstance == NULL)
                    {
                            m_pInstance = new ZKClientMgr;
                    }

                    return m_pInstance;
            }

    private:
            ZKClientMgr(){};
            static ZKClientMgr* m_pInstance;
};

#endif

