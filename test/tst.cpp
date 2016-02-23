#include "wrapper_curl.h"
#include "include/rapidjson/writer.h"
#include "include/rapidjson/stringbuffer.h"
#include <string>
#include <iostream>

using namespace std;

int main(void)
{
    cCurl objCurl;
    bool ret = objCurl.Init();
    if (!ret)
    {
        cout << "cCurl Init failed.\n";
        return 0;
    } 

    string sUrl = "http://api.weixin.oa.com/itilalarmcgi/sendmsg";
    rapidjson::StringBuffer s;
    rapidjson::Writer<rapidjson::StringBuffer> writer(s);
    string buf("data=");
    writer.StartObject();
    writer.Key("Sender");
    writer.String("v_monitor");
    writer.Key("Rcptto");
    writer.StartArray();
    writer.String("willrlzhang");

    writer.EndArray();
    writer.Key("isText");
    writer.String("hello world");
    writer.EndObject();

    buf += s.GetString();
    ret = objCurl.DoPerform(sUrl, buf);
    if (!ret)
    {
        cout << "cCurl DoPerform faield.\n";
        return 0;
    }
    
    return 0;
}

