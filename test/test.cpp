#include<iostream>
#include "3rd_wrapper.h"

using namespace std;

int main(void)
{
	string sList("--SERVER=localhost:11211");
	Memcached mem;
	mem.Init(sList);
	string key("key2");
	string value("value2");
	time_t tOverTime = 180;
	uint32_t uFlag = 0;
	
	if (!mem.Set(key, value,tOverTime, uFlag))
	{
		cout << "set failed.\n";
		cout << mem.GetErrMsg();
		return -1;
	}

	size_t vLen;
	uint32_t retFlag;
	string ret = mem.Get(key, vLen, retFlag);
	if (ret.empty() )
	{
		cout << "Get failed.\n";
		cout << mem.GetErrMsg();
		return -1;
	}

	cout << "value: " << ret << endl;
	return 0;
}
