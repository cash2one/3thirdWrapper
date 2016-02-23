#ifndef __WRAPPER_CURL_H__
#define __WRAPPER_CURL_H__

#include "curl/curl.h"
#include <string>

class cCurl
{
    public:
        cCurl():m_curl(NULL), m_ret(CURLE_OK)
        {
        }

        bool Init()
        {
            curl_global_init(CURL_GLOBAL_ALL);
            m_curl = curl_easy_init();
            if (m_curl == NULL)
            {
                m_sErrMsg = "curl_easy_init return NULL";
                return false;
            }

            m_ret = curl_easy_setopt(m_curl, CURLOPT_POST, 1);
            if (m_ret != CURLE_OK)
            {
                m_sErrMsg = curl_easy_strerror(m_ret);
                return false;
            }

            m_ret = curl_easy_setopt(m_curl, CURLOPT_WRITEFUNCTION, &WriteData);
            if (m_ret != CURLE_OK)
            {
                m_sErrMsg = curl_easy_strerror(m_ret);
                return false;
            }
            m_ret = curl_easy_setopt(m_curl, CURLOPT_WRITEDATA, &m_sWriteBuf);
            if (m_ret != CURLE_OK)
            {
                m_sErrMsg = curl_easy_strerror(m_ret);
                return false;
            }
            m_ret = curl_easy_setopt(m_curl, CURLOPT_FOLLOWLOCATION, 1);
            if (m_ret != CURLE_OK)
            {
                m_sErrMsg = curl_easy_strerror(m_ret);
                return false;
            }

            return true;
        }

        std::string& GetWriteBuf()
        {
            return m_sWriteBuf;
        }

        bool DoPerform(std::string& sUrl, std::string& sFields)
        {
            m_sUrl = sUrl;
            m_sFields = sFields;

            if (m_curl == NULL)
            {
                m_sErrMsg = "DoPerform failed, m_curl is NULL";
                return false;
            }

            curl_easy_setopt(m_curl, CURLOPT_URL, m_sUrl.c_str());
            curl_easy_setopt(m_curl, CURLOPT_POSTFIELDS, m_sFields.c_str());
            m_ret = curl_easy_perform(m_curl);
            if (m_ret != CURLE_OK)
            {
                m_sErrMsg = curl_easy_strerror(m_ret);
                return false;
            }

            return true;
        }

        static size_t  WriteData(void* buf, size_t size, size_t nmemb, void* userp)
        {
            char* pSrc = (char* )buf;
            std::string* pDst = (std::string* )userp;
            size_t ret = 0;

            if (pDst != NULL)
            {
                pDst->append(pSrc, size*nmemb);
                ret = size*nmemb;
            }

            return ret;
        }

        virtual ~cCurl()
        {
            if (m_curl != NULL)
            {
                curl_easy_cleanup(m_curl);
            }

            curl_global_cleanup();
        }

        std::string GetErrMsg()
        {
            return m_sErrMsg;
        }

    private:
        CURL* m_curl;
        CURLcode m_ret;

        std::string m_sUrl;
        std::string m_sFields;
        std::string m_sWriteBuf;
        std::string m_sErrMsg;
};

#endif

