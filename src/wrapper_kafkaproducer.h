#ifndef __KAFKAPRODUCER__H
#define __KAFKAPRODUCER__H

#include <string>
#include <map>
#include "librdkafka/rdkafkacpp.h"

class KafkaProducer 
{
    public:
        bool InIt(std::string& sBrokers)
        {
            std::string strErr;
            m_pconf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
            m_ptconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
            m_pconf->set("metadata.broker.list", strBrokers, strErr);
            m_pconf->set("queue.buffering.max.messages", "10000000", strErr);
            m_pconf->set("producer.type", "sync", strErr);
            m_pproducer = RdKafka::Producer::create(m_pconf, strErr);
            if (m_pproducer == NULL)
            {
                return false;
            }

        }

    KafkaProducer()
    {}

    ~KafkaProducer()
    {
        if (m_pproducer != NULL)
        {
            m_pproducer->poll(1000);
            delete m_pproducer;
            m_pproducer = NULL;
        }

        std::map<std::string, RdKafka::Topic*>::iterator itr = m_maptopic.begin();
        for (; itr != m_maptopic.end(); ++ itr)
        {
            if (itr->second != NULL)
            {
                delete itr->second;
                itr->second = NULL;
            }
        }

        if (m_pconf != NULL)
        {
            delete m_pconf;
            m_pconf = NULL;
        }
        if (m_ptconf != NULL)
        {
            delete m_ptconf;
            m_ptconf = NULL;
        }
        RdKafka::wait_destroyed(2000);
    };

    bool addTopic(std::string& strTopic)
    {
        if (m_maptopic.find(strTopic) != m_maptopic.end())
        {
            return true;
        }

        if (m_pproducer == NULL)
        {
            return false;
        }

        std::string strErr;
        RdKafka::Topic* ptopic = RdKafka::Topic::create(m_pproducer, strTopic, m_ptconf, strErr);
        if (ptopic == NULL)
        {
            return false;
        }
        m_maptopic[strTopic] = ptopic;
        return true;
    }

    bool produce(std::string& strTopic, int partId, std::string& strMsg)
    {
        if (m_maptopic.find(strTopic) == m_maptopic.end())
        {
            return false;
        }

        if (m_pproducer == NULL || m_maptopic[strTopic] == NULL)
        {
            return false;
        }

        RdKafka::ErrorCode resp = m_pproducer->produce(m_maptopic[strTopic], partId,
                              RdKafka::Producer::MSG_COPY,
                              const_cast<char *>(strMsg.data()), strMsg.size(),
                              NULL, NULL);

        if (resp != RdKafka::ERR_NO_ERROR)
        {
            return false;
        }
        return true;
    }

    private:
    RdKafka::Conf* m_pconf;
    RdKafka::Conf* m_ptconf;
    RdKafka::Producer* m_pproducer;
    std::map<std::string, RdKafka::Topic*> m_maptopic;
};

#endif

