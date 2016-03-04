#include <iostream>
#include <string>
#include "librdkafka/rdkafkacpp.h"
#include <map>
#include <utility>

class cKafkaConsume
{
    public:
        cKafkaConsume()
        {}

        ~cKafkaConsume()
        {
            std::map<std::string, RdKafka::Topic*>::iterator iter;
            for (iter=m_mapTopics.begin(); iter!=m_mapTopics.end(); iter++)
            {
                if (iter->second)
                {
                    m_Consumer->stop(iter->second, 0);
                }
            }

            m_Consumer->poll(1000);
            for (iter=m_mapTopics.begin(); iter!=m_mapTopics.end(); iter++)
            {
                if (iter->second)
                {
                    delete iter->second;
                    iter->second = NULL;
                }
            }

            delete m_Consumer;
        }

        bool Init(std::string& sBrokerList)
        {
            std::string sMsg;
            m_GloalbConf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
            if (m_GloalbConf == NULL)
            {
                return false;
            }

            m_TopicConf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
            if (m_TopicConf == NULL)
            {
                return false;
            }

            m_GloalbConf->set("metadata.broker.list", sBrokerList, sMsg);
            m_GloalbConf->set("default_topic_conf", m_TopicConf, sMsg);

            m_Consumer = RdKafka::Consumer::create(m_GloalbConf, sMsg);
            if (m_Consumer == NULL)
            {
                m_sErrMsg = "create consumer failed. errmsg:" + sMsg;
                return false;
            }

            return true;
        }

        bool ConsumerReading(std::string& sTopic, int64_t start_offset=RdKafka::Topic::OFFSET_END)
        {
            RdKafka::Topic* ptr = NULL;
            std::map<std::string, RdKafka::Topic*>::iterator iter = m_mapTopics.find(sTopic);
            if (iter == m_mapTopics.end())
            {
                std::string sMsg;
                ptr = RdKafka::Topic::create(m_Consumer, sTopic, m_TopicConf, sMsg);
                if (ptr == NULL)
                {
                    m_sErrMsg = "Creaet Topic in Consumer failed. errmsg:" + m_sErrMsg;
                    return false;
                }
                m_mapTopics.insert(std::make_pair<std::string, RdKafka::Topic*>(sTopic, ptr));
            }
            else
            {
                ptr = iter->second;
            }

            RdKafka::ErrorCode resp = m_Consumer->start(ptr, 0, start_offset-10);
            if (resp != RdKafka::ERR_NO_ERROR)
            {
                m_sErrMsg = "Consumer start failed. errmsg:" + RdKafka::err2str(resp);
                return false;
            }

            RdKafka::Message* msg = m_Consumer->consume(ptr, 0, 1000);
            MsgConsume(msg, NULL);
            delete msg;
            return true;
        }

    private:
        bool DoMsg(RdKafka::Message* msg)
        {
            std::cout <<"Read msg at offset " << msg->offset() << std::endl;
            if (msg->key())
            {
                std::cout <<"msg key: " << *msg->key() << std::endl;
            }

            std::cout << "msg len: " << static_cast<int>(msg->len()) << "\t msg :" << static_cast<char* >(msg->payload()) << std::endl;
            return true;
        }

       bool MsgConsume(RdKafka::Message* msg,  void* opt)
       {
           switch (msg->err())
           {
               case RdKafka::ERR__TIMED_OUT:
                   std::cout <<"MsgConsume msg time out\n";
                   break;

               case RdKafka::ERR_NO_ERROR:
                   DoMsg(msg);           
                   break;

               case RdKafka::ERR__PARTITION_EOF:
                   break;

               case RdKafka::ERR__UNKNOWN_TOPIC:
               case RdKafka::ERR__UNKNOWN_PARTITION:
               default:
                   std::cout << "msg consume failed, errmsg: " << msg->errstr() << std::endl;
                   break;
           }

           return true;
       } 

    private:
        std::string m_sBrokeList;
        std::string m_sErrMsg;
        RdKafka::Conf *m_GloalbConf;
        RdKafka::Conf *m_TopicConf;
        RdKafka::Consumer* m_Consumer;
        std::map< std::string, RdKafka::Topic* > m_mapTopics;
};

class cKafkaMeta
{
    public:
       cKafkaMeta()
       {} 

       ~cKafkaMeta()
       {}

       bool Init()
       {
           std::string sMsg;
           RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
           m_Producer = RdKafka::Producer::create(conf, sMsg);
           if (m_Producer == NULL)
           {
               return false;
           }
       }

       bool DoMetaData(std::string& sTopic)
       {
           RdKafka::Topic* ptr = NULL;
           std::map<std::string, RdKafka::Topic*>::iterator iter = m_mapTopics.find(sTopic);
           if (iter != m_mapTopics.end())
           {
                ptr = iter->second; 
           }
           else
           {
                std::string sMsg;
                //ptr = RdKafka::Topic::create(m_Producer, sTopic,  , sMsg):
           }

           class RdKafka::Metadata* meta;
           RdKafka::ErrorCode resp ;//= m_Producer->metadata(sTopic!=NULL, sTopic, &meta, 5000);
           if (resp != RdKafka::ERR_NO_ERROR)
           {
               std::cout <<"metadata failed. errmsg: " << RdKafka::err2str(resp) << std::endl;
               return false;
           }
       }

    private:
       RdKafka::Producer* m_Producer;
       std::map<std::string, RdKafka::Topic*> m_mapTopics;
       

};

int main(void)
{
    cKafkaConsume consumer;
    std::string sBrokerList = "10.120.88.199:9092,10.120.88.200:9092,10.120.88.201:9092,10.120.88.202:9092,10.120.88.203:9092";
    std::string sTopic = "kafka_1023";
    consumer.Init(sBrokerList);
    consumer.ConsumerReading(sTopic);

    return 0;
}

