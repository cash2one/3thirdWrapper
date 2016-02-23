#!/bin/bash

function init_para() 
{
	# 程序名称
	prag_name=$0

	# 程序接收的参数个数
	prag_para_num=$#

	# 日期(提供三种格式)
	date1=`date +%Y-%m-%d`
	date2=`date +%Y_%m_%d`
	date3=`date +%Y%m%d`

	# 脚本所在的路径
	prag_path=`dirname $prag_name`
	prag_basename=`basename $prag_name`

	# 作者(默认为 willrlzhang)
	auto_author="willrlzhang"

	# 公共头文件的名称
	auto_header_name="comm_header.h"
}

function usage() 
{
	echo -e "\tUsage: $prag_name [comm_head_file] [author]"
}

# 初始化参数
init_para

# 检查参数
if [ $# -lt 2 ]; then
	echo "Error! Too less parameters! $prag_para_num"
	usage
	exit 1
fi

# 具体实现
auto_header_name="$1"
auto_author="$2"
micro="__${auto_header_name%%.h}_h_$date2_$auto_author"__"" 

line_num=`ls -l *.h 2>/dev/null | wc -l `
if [ "$line_num"x == "0"x ]; then
	echo "None of header file exists! Please check current path."
	exit 2
fi

echo -e "#ifndef $micro" > $auto_header_name
echo -e "#define $micro\n" >> $auto_header_name

ls *.h | while read header
do
	echo "#include \"$header\""
done >> $auto_header_name

echo -e "\n#endif" >> $auto_header_name
