#!/bin/bash

if [ $1 = "join" ]; then
	python3 Membership.py &
	python3 Election.py &
	python3 DataNode.py &
elif [ $1 = "leave" ]; then
	pkill python3
elif [ $1 = "list_mem" ]; then
	python3 tools.py "list_mem"
elif [ $1 = "list_self" ]; then
	python3 tools.py "list_self"
elif [ $1 = "list_pred" ]; then
	python3 tools.py "list_pred"
elif [ $1 = "list_succ1" ]; then
	python3 tools.py "list_succ1"
elif [ $1 = "list_succ2" ]; then
	python3 tools.py "list_succ2"
elif [ $1 = "set_loss_3" ]; then
	python3 tools.py "set_loss_3"
elif [ $1 = "set_loss_30" ]; then
	python3 tools.py "set_loss_30"
elif [ $1 = "set_loss_0" ]; then
	python3 tools.py "set_loss_0"
elif [ $1 = "get_loss" ]; then
	python3 tools.py "get_loss"
elif [ $1 = "put" ]; then
	python3 Client.py "WR" $2 $3
elif [ $1 = "get" ]; then
	python3 Client.py "RR" $3 $2
elif [ $1 = "delete" ]; then
	python3 Client.py "DR" '_' $2
elif [ $1 = 'ls' ]; then
	python3 tools.py 'ls' $2
elif [ $1 = 'store' ]; then
	python3 tools.py 'store'
elif [ $1 = 'get_version' ]; then
	python3 tools.py 'get_version' $2 $3 $4
elif [ $1 = 'C1' ]; then
	python3 tools.py 'C1'
elif [ $1 = 'C2' ]; then
	python3 tools.py 'C2'
elif [ $1 = 'C3' ]; then
	python3 tools.py 'C3' $2 $3
elif [ $1 = 'C5' ]; then
	python3 tools.py 'C5'
elif [ $1 = 'C4' ]; then
	python3 tools.py 'C4' $2
fi;
