200.times do |i|
	%x( echo "begin\ninsert into abc values (#{i},Puppa,NOW)\ncommit" | telnet localhost 8000 )
end
