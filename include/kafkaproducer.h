#ifndef __KAFKAPRODUCER__H
#define __KAFKAPRODUCER__H

#include <string>
#include <map>
#include "librdkafka/rdkafkacpp.h"
#include "log.h"

class KafkaProducer 
{
    public:
    KafkaProducer(std::string& strBrokers)
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
            LOG_ERROR("<KafkaProducer> Failed to create producer: %s\n", strErr.c_str());
        }
    };

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
            LOG_ERROR("<addTopic> topic exist: %s\n", strTopic.c_str());
            return true;
        }

        if (m_pproducer == NULL)
        {
            LOG_ERROR("<addTopic> producer is NULL\n");
            return false;
        }

        std::string strErr;
        RdKafka::Topic* ptopic = RdKafka::Topic::create(m_pproducer, strTopic, m_ptconf, strErr);
        if (ptopic == NULL)
        {
            LOG_ERROR("<addTopic> Failed to create topic: %s\n", strErr.c_str());
            return false;
        }
        m_maptopic[strTopic] = ptopic;
        return true;
    }

    bool produce(std::string& strTopic, int partId, std::string& strMsg)
    {
        if (m_maptopic.find(strTopic) == m_maptopic.end())
        {
            LOG_ERROR("<produce> topic not exist\n");
            return false;
        }

        if (m_pproducer == NULL || m_maptopic[strTopic] == NULL)
        {
            LOG_ERROR("<produce> producer or topic invalid\n");
            return false;
        }

        RdKafka::ErrorCode resp = m_pproducer->produce(m_maptopic[strTopic], partId,
                              RdKafka::Producer::MSG_COPY,
                              const_cast<char *>(strMsg.data()), strMsg.size(),
                              NULL, NULL);

        if (resp != RdKafka::ERR_NO_ERROR)
        {
            LOG_ERROR("<produce> send msg failed: %s\n",
                RdKafka::err2str(resp).c_str());
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

