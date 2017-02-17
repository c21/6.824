for i in {1..100}
do
	echo $i
	go test -run $1 2>output
	rc=`echo $?`
	if [ $rc == "1" ]
	then
		break
	fi
done
