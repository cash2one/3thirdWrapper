#include <iostream>
#include <string>
#include "librdkafka/rdkafkacpp.h"
#include <map>
#include <utility>
#include <string.h>
#include <stdio.h>

class cFunConsumer
{
    public:
        ~cFunConsumer()
        {}
        virtual bool operator ()(RdKafka::Message*)
        {}
};

class cFunc:public cFunConsumer
{
    bool operator()(RdKafka::Message* msg)
    {
        std::cout <<"Read msg at offset " << msg->offset() << std::endl;
        if (msg->key())
        {
            std::cout <<"msg key: " << *msg->key() << std::endl;
        }

        std::string sMsg(static_cast<char* >(msg->payload()), static_cast<int>(msg->len()));
        std::cout << sMsg << std::endl;

        return true;
    }
};

class cKafkaConsume
{
    public:
        cKafkaConsume()
        {
            m_GloalbConf = NULL;
            m_TopicConf = NULL;
            m_Consumer = NULL;
            m_bRun = true;
        }

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

        bool ConsumerReading(std::string& sTopic, int64_t start_offset=RdKafka::Topic::OFFSET_BEGINNING)
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

            RdKafka::ErrorCode resp = m_Consumer->start(ptr, 0, start_offset);
            if (resp != RdKafka::ERR_NO_ERROR)
            {
                m_sErrMsg = "Consumer start failed. errmsg:" + RdKafka::err2str(resp);
                return false;
            }

            while (m_bRun)
            {
                RdKafka::Message* msg = m_Consumer->consume(ptr, 0, 10000);
                char buf[128];
                strncpy(buf, sTopic.c_str(),  sTopic.size());
                buf[sTopic.size()] = '\0';
                MsgConsume(msg, buf);
                delete msg;
                msg = NULL;
                m_Consumer->poll(0);
            }

            return true;
        }

        void SetCBFunc(std::string& sTopic, cFunConsumer* obj)
        {
            if (!sTopic.empty())
            {
                m_mapCBFunc.insert(std::make_pair(sTopic, obj)); 
            }
        }

    private:

       bool MsgConsume(RdKafka::Message* msg,  void* opt)
       {
           std::map<std::string, cFunConsumer*>::iterator iter;
           std::string s = (char*)opt;
           switch (msg->err())
           {
               case RdKafka::ERR__TIMED_OUT:
                   std::cout <<"MsgConsume msg time out\n";
                   break;

               case RdKafka::ERR_NO_ERROR:
                   //DoMsg(msg);           
                   iter = m_mapCBFunc.find(s);
                   if (iter != m_mapCBFunc.end())
                   {
                       if (iter->second != NULL)
                       {
                           (*iter->second)(msg);
                       }
                   } 
                   break;

               case RdKafka::ERR__PARTITION_EOF:
                   std::cout <<"partition eof\n";
                   //m_bRun = false;
                   break;

               case RdKafka::ERR__UNKNOWN_TOPIC:
               case RdKafka::ERR__UNKNOWN_PARTITION:
               default:
                   std::cout << "msg consume failed, errmsg: " << msg->errstr() << std::endl;
                   m_bRun = false;
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
        bool m_bRun;
        std::map<std::string, cFunConsumer*> m_mapCBFunc;
};

class cKafkaMeta
{
    public:
       cKafkaMeta()
       {
           m_Conf = NULL;
           m_TConf = NULL;
           m_Producer = NULL;
           m_mapTopics.clear();
       } 

       ~cKafkaMeta()
       {}

       bool Init(std::string& sBrokerList)
       {
           std::string sMsg;
           RdKafka::Conf *m_Conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
           if (m_Conf == NULL)
           {
               return false;
           }

           m_Conf->set("metadata.broker.list", sBrokerList, sMsg);
           RdKafka::Conf *m_TConf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
           if (m_TConf == NULL)
           {
               return false;
           }

           m_Conf->set("default_topic_conf", m_TConf, sMsg);
           m_Producer = RdKafka::Producer::create(m_Conf, sMsg);
           if (m_Producer == NULL)
           {
               return false;
           }
       }

       bool DoMetaData(std::string& sTopic)
       {
           RdKafka::Topic* ptr = NULL;
           if (!sTopic.empty())
           {
                std::map<std::string, RdKafka::Topic*>::iterator iter = m_mapTopics.find(sTopic);
                if (iter != m_mapTopics.end())
                {
                     ptr = iter->second; 
                }
                else
                {
                     std::string sMsg;
                     ptr = RdKafka::Topic::create(m_Producer, sTopic,  m_TConf, sMsg);
                     if (ptr != NULL)
                     {
                         m_mapTopics.insert(std::make_pair<std::string, RdKafka::Topic*>(sTopic, ptr));
                     }
                }

           }

           class RdKafka::Metadata* meta;
           RdKafka::ErrorCode resp = m_Producer->metadata(sTopic.empty(), ptr, &meta, 5000);
           if (resp != RdKafka::ERR_NO_ERROR)
           {
               std::cout <<"metadata failed. errmsg: " << RdKafka::err2str(resp) << std::endl;
               return false;
           }

           PrintMetaData(sTopic, meta);

           delete meta;
       }

    private:
       void PrintMetaData(std::string& sTopic, RdKafka::Metadata* meta)
       {
            std::cout <<"Metadata for " << (sTopic.empty()?"All Topics":sTopic) << "(from broker " << meta->orig_broker_id() << ":" << meta->orig_broker_name() << std::endl;            
            
            //brokers
            std::cout << " " << meta->brokers()->size() << " brokers:" << std::endl;
            
            RdKafka::Metadata::BrokerMetadataIterator ib;
            for (ib = meta->brokers()->begin(); ib!=meta->brokers()->end(); ib++)
            {
                 std::cout << "  broker " << (*ib)->id() << " at " << (*ib)->host() << ":" << (*ib)->port() << std::endl;
            }

            //topics
            std::cout << meta->topics()->size() << " topics:" << std::endl;
            RdKafka::Metadata::TopicMetadataIterator it;
            for (it = meta->topics()->begin(); it!=meta->topics()->end(); it++)
            {
                 std::cout << "  topic \""<< (*it)->topic() << "\" with " << (*it)->partitions()->size() << " partitions:";
                 if ((*it)->err() != RdKafka::ERR_NO_ERROR)
                 {
                     std::cout << " " << err2str((*it)->err());
                     if ((*it)->err() == RdKafka::ERR_LEADER_NOT_AVAILABLE)
                     {
                         std::cout << " (try again)";
                     }
                 }

                std::cout <<std::endl;

                //topic's partitions 
                RdKafka::TopicMetadata::PartitionMetadataIterator ip;
                for (ip = (*it)->partitions()->begin();ip != (*it)->partitions()->end();ip++)
                {
                    std::cout << "    partition " << (*ip)->id() << ", leader " << (*ip)->leader() << ", replicas: ";
                    //partition's replicas 
                    RdKafka::PartitionMetadata::ReplicasIterator ir;
                    for (ir = (*ip)->replicas()->begin();ir != (*ip)->replicas()->end();++ir)
                    {
                        std::cout << (ir == (*ip)->replicas()->begin() ? "":",") << *ir;
                    }

                    //partition's ISRs
                    std::cout << ", isrs: ";
                    RdKafka::PartitionMetadata::ISRSIterator iis;
                   for (iis = (*ip)->isrs()->begin(); iis != (*ip)->isrs()->end() ; ++iis)
                   {
                       std::cout << (iis == (*ip)->isrs()->begin() ? "":",") << *iis;
                       if ((*ip)->err() != RdKafka::ERR_NO_ERROR)
                       {
                           std::cout << ", " << RdKafka::err2str((*ip)->err()) << std::endl;
                       }
                       else
                       {
                           std::cout << std::endl;
                       }
                   } 
                }
            }

       }

    private:
       RdKafka::Producer* m_Producer;
       RdKafka::Conf* m_Conf;
       RdKafka::Conf* m_TConf;

       std::map<std::string, RdKafka::Topic*> m_mapTopics;
};

class cProducer
{
    public:
        cProducer()
        {
            m_Producer = NULL;
            m_Conf = NULL;
            m_TConf = NULL;
            m_mapTopics.clear();
        }

        ~cProducer()
        {
            std::map<std::string, RdKafka::Topic*>::iterator iter;
            for (iter=m_mapTopics.begin(); iter!=m_mapTopics.end(); iter++)
            {
                if (iter->second != NULL)
                {
                    delete iter->second ;
                    iter->second = NULL;
                }
            }

            if (m_Conf != NULL)
            {
                delete m_Conf;
                m_Conf = NULL;
            }

            if (m_TConf != NULL)
            {
                delete m_TConf;
                m_TConf = NULL;
            }

            if (m_Producer != NULL)
            {
                m_Producer->poll(1000);
                delete m_Producer;
                m_Producer = NULL;
            }

            RdKafka::wait_destroyed(3000);
        }

        bool Init(std::string& sBrokerList)
        {
            std::string sMsg;
            m_Conf= RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
            if (m_Conf == NULL)
            {
                return false;
            }

            m_TConf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
            if (m_TConf == NULL)
            {
                return false;
            }

            m_Conf->set("metadata.broker.list", sBrokerList, sMsg);
            m_Conf->set("default_topic_conf", m_TConf, sMsg);

            m_Producer= RdKafka::Producer::create(m_Conf, sMsg);
            if (m_Producer== NULL)
            {
                m_sErrMsg = "create consumer failed. errmsg:" + sMsg;
                return false;
            }

            return true;
        }

        bool AddTopic(std::string sTopic)
        {
            RdKafka::Topic* ptr;
            if (m_mapTopics.find(sTopic) != m_mapTopics.end())
            {
                std::cout <<"topic is exist\n";
                return true;
            } 

            if (m_Producer == NULL)
            {
                std::cout <<"Add Topic failed.\n";
                return false;
            }

            std::string sMsg;
            ptr = RdKafka::Topic::create(m_Producer, sTopic, m_TConf, sMsg);
            if (ptr == NULL)
            {
                m_sErrMsg = "AddTopic failed, errmsg:" +  sMsg;
                return false;
            }

            m_mapTopics[sTopic] = ptr;

            return true;
        }

        bool Produce(std::string& sTopic, std::string& sMsg, int iPartId=RdKafka::Topic::PARTITION_UA)
        {
            if (m_mapTopics.find(sTopic) == m_mapTopics.end())
            {
                m_sErrMsg = "Produce failed,m_mapTopics not find topic: " + sTopic;
                return false;
            }

            if (m_Producer == NULL || m_mapTopics[sTopic] == NULL)
            {
                return false;
            }

            RdKafka::ErrorCode resp = m_Producer->produce(m_mapTopics[sTopic], iPartId, RdKafka::Producer::MSG_COPY, const_cast<char*>(sMsg.data()), sMsg.size(), NULL, NULL);
            if (resp != RdKafka::ERR_NO_ERROR)
            {
                m_sErrMsg = "Produce failed. errmsg: " + RdKafka::err2str(resp);
                return false;
            }

            return true;
        }

    private:
        std::string m_sErrMsg;
        RdKafka::Producer* m_Producer;
        RdKafka::Conf* m_Conf;
        RdKafka::Conf* m_TConf;
        std::map<std::string, RdKafka::Topic*> m_mapTopics;
};

int main(void)
{
    //cProducer producer;
    cKafkaConsume consumer;
    std::string sBrokerList = "10.120.88.199:9092,10.120.88.200:9092,10.120.88.201:9092,10.120.88.202:9092,10.120.88.203:9092";
    std::string sTopic = "kafka_test";

    /*
    producer.Init(sBrokerList);
    producer.AddTopic(sTopic);
    std::string s = "hello world";
    std::string sMsg;
    char buf[12] ;
    for (int i=0; i<10; i++)
    {
        memset(buf, 0, sizeof(buf));
        snprintf(buf, sizeof(buf), "%d", i);
        sMsg = s +  buf;
        producer.Produce(sTopic, sMsg);

    }
    */

    consumer.Init(sBrokerList);
    cFunConsumer* func = new cFunc;
    consumer.SetCBFunc(sTopic, func);
    consumer.ConsumerReading(sTopic);
    delete func;
    func = NULL;

    return 0;
    
    cKafkaMeta meta;
    meta.Init(sBrokerList);
    meta.DoMetaData(sTopic);
    
    return 0;
}

