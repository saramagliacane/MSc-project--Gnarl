%% Author: Sara Magliacane
%% Created: 29/ago/2010
%% Description: TODO: Add description to utility
-module(utility).
%% retrasmission timeout in sendAckAndWait
-define(rto_timeout, 400).
%% timeout to wait for all acks in multicast
-define(acktimeout, 1000).
-define(unicasttimeout, 3000).
-define(multicasttimeout, 5000).
-define(NRETRASMITS, 1).
-define(VERBOSE, 2).

%%
%% Imported Functions
%%
-import(errorhandler, [on_exit/5, sendStateToSupervisor/3, onSenderExit/8]).

%%
%% Exported Functions
%%
-export([returnMsgsToBeElaborated/3, checkFather/1, printVerboseLevel/3, sendAndWaitLoop/5, multicastSendLoop/7, unicastSendLoop/4,
		 removeFromLastIdVector/2, incrementLastSentId/2, getLastSentIdFor/2]).

-export([remoteIsProcessAlive/1, remotewhereis/2, registerName/2, unicastSendWithoutAck/4,
	 remoteIsRegNameAlive/2, remoteIsRegNameAlive/1, unicastSend/4, multicastSend/4]).



%%
%% API Functions
%%

%%% ******************* Network utilities ****************************
%%%
%%% Multicast Sending of Messages in order to make the channels FIFO with IDs and ACKs
%%% Each message trasmission is managed by a separate process, sendAndWaitForAck, which
%%%	waits ?rto_timeout time for ACKs, or resends the message
%%%
unicastSendWithoutAck(SenderInfo, Message, ReceiverInfo,  LastSentIdVector)->
	printVerboseLevel("~n~w@~s> Sender message without ack, message: ~w, ReceiverInfo: ~w ~n", 
					 [SenderInfo, node(), Message, ReceiverInfo], 1),
	%% todo fix
	NewLastSentIdVector = incrementLastSentId(ReceiverInfo, LastSentIdVector),
	MsgId = getLastSentIdFor(ReceiverInfo,NewLastSentIdVector),
	spawn(node(), fun()-> sendAndWaitLoop({noreply}, SenderInfo, Message, ReceiverInfo, MsgId)end),
	NewLastSentIdVector.
	

unicastSend(SenderInfo, Message, ReceiverInfo,  LastSentIdVector)->
	NewLastSentIdVector = incrementLastSentId(ReceiverInfo, LastSentIdVector),
	MsgId = getLastSentIdFor(ReceiverInfo,LastSentIdVector),
	Pid = spawn(node(), fun()-> unicastSendLoop(SenderInfo, Message, ReceiverInfo,  NewLastSentIdVector)end),
	onSenderExit(Pid, node(), unicast, not_important, SenderInfo, Message, ReceiverInfo, MsgId),
	NewLastSentIdVector.
	

unicastSendLoop(SenderInfo, Message, ReceiverInfo,  LastSentIdVector)->
	MsgId = getLastSentIdFor(ReceiverInfo,LastSentIdVector),
	MulticastBoss = self(),
	printVerboseLevel("~n~w@~s> Sender message waiting for ack, Pid: ~w, ReceiverInfo: ~w ~n", 
					 [SenderInfo, node(), self(), ReceiverInfo], 5),
	spawn(node(), fun()-> sendAndWaitLoop(MulticastBoss, SenderInfo, Message, ReceiverInfo, MsgId)end),
	receive
		{ack_message, ReceiverInfo, MsgId, Message} ->
			printVerboseLevel("~n~s ---- Completed unicast for sender: ~w, receiver: ~w , message: ~w~n", 
					  [node(),SenderInfo, ReceiverInfo, Message], 5),
			SenderInfo! {unicast_completed, ReceiverInfo, MsgId, Message};
		Any-> printVerboseLevel("~n~s ---- Unexpected:~w, while waiting unicast for sender: ~w, receiver: ~w , message: ~w~n", 
					  [node(),Any, SenderInfo, ReceiverInfo, Message], 1)
	after ?unicasttimeout ->
			printVerboseLevel("~n~s ---- Didn't complete unicast for sender: ~w, receiver: ~w , message: ~w~n", 
					  [node(),SenderInfo, ReceiverInfo, Message], 1),
			SenderInfo! {unicast_not_completed, ReceiverInfo, MsgId, Message}
	end,
	timer:sleep(50). 


	
multicastSend(_SenderInfo, _Message, [], LastSentIdVector)->
	LastSentIdVector;
multicastSend(SenderInfo, Message, ReceiversList, LastSentIdVector) ->
	NewLastSentIdVector = incrementAllLastSentId(ReceiversList, LastSentIdVector),
	Pid = spawn(node(), fun()->
			multicastSendLoop(SenderInfo, Message, ReceiversList, NewLastSentIdVector, [], ReceiversList, LastSentIdVector)
		  end),
	onSenderExit(Pid, node(), multicast, not_important, SenderInfo, Message, ReceiversList, NewLastSentIdVector),
	NewLastSentIdVector.
	


multicastSendLoop( SenderInfo,Message,[],  _LastSentIdVector, notcompleted, OldReceiversList, OldLastSentIdVector) -> 
	%% didn't receive ack for all sent processes
	SenderInfo! {multicast_not_completed, OldReceiversList, Message, OldLastSentIdVector};

multicastSendLoop( SenderInfo,Message,[],  _LastSentIdVector, [], OldReceiversList, OldLastSentIdVector) -> 
	%% received ack for all sent processes
	SenderInfo! {multicast_completed, OldReceiversList, Message, OldLastSentIdVector};

multicastSendLoop( SenderInfo, Message,[],  LastSentIdVector, WaitingForAck, OldReceiversList, OldLastSentIdVector) -> 
	receive
		{ack_message, ReceiverInfo, MsgId, Message} -> 
			case lists:keyfind(ReceiverInfo,1,WaitingForAck) of
				{ReceiverInfo, MsgId}->
					printVerboseLevel("~n~s ---- Received ACK for sender: ~w, receiver: ~w , message: ~w~n", 
					  [node(),SenderInfo, ReceiverInfo, Message], 5),
					NewWaitingForAck = WaitingForAck -- [{ReceiverInfo,MsgId}];
				Any-> 
					printVerboseLevel("~n~s ----- Multicast send received  unexpected ack: ~w~n", 
					  [node(),Any], 1),
					NewWaitingForAck = WaitingForAck 
			end;
		Any ->
			printVerboseLevel("~n~s ------ Multicast send received  unexpected message: ~w~n", 
					  [node(),Any], 1), NewWaitingForAck = WaitingForAck
			
	after ?acktimeout ->
		printVerboseLevel("~n~s ---- Didn't receive ACK for sender: ~w, message: ~w, WaitingForAck:~w~n", 
					  [node(),SenderInfo, Message, WaitingForAck], 1),
		NewWaitingForAck = notcompleted
	end,
	multicastSendLoop( SenderInfo, Message,[],  LastSentIdVector, NewWaitingForAck, OldReceiversList, OldLastSentIdVector);

multicastSendLoop(SenderInfo, Message, [{ReceiverInfo,_MatchingFunctions}|T], LastSentIdVector, WaitingForAck, OldReceiversList, OldLastSentIdVector) ->	
	MsgId = getLastSentIdFor(ReceiverInfo,LastSentIdVector),
	MulticastBoss = self(),
	spawn(node(), fun()-> sendAndWaitLoop(MulticastBoss, SenderInfo, Message, ReceiverInfo, MsgId)end),
	multicastSendLoop(SenderInfo,Message,T, LastSentIdVector, WaitingForAck ++ [{ReceiverInfo,MsgId}], OldReceiversList, OldLastSentIdVector).



%%%
%%% Separate process managing each message - now with error handlers :)
%%%

sendAndWaitLoop( MulticastBoss, SenderInfo, Message, ReceiverInfo, MsgId)->

	printVerboseLevel("~n~s Trying to send from: ~w, to: ~w, message: ~w, msgId:~w, MulticastBoss:~w~n", 
					  [node(),SenderInfo,ReceiverInfo,  Message, MsgId, MulticastBoss ], 5),
try
	{ReceiverName, ReceiverNode} = ReceiverInfo,	
	
	%% is the Receiver alive?
    case utility:remoteIsRegNameAlive(ReceiverName, ReceiverNode) of
			false -> throw(receiver_crashed);
			true ->	ReceiverInfo ! {MulticastBoss,SenderInfo,MsgId,Message}
	end

%% 	,{SenderName, SendererNode} = SenderInfo,
%%  %% is the Sender alive?
%% 	case utility:remoteIsRegNameAlive(SenderName, SendererNode) of
%% 			false -> throw(sender_crashed);
%% 			true ->	ok
%% 	end
		
catch
%% 	sender_crashed-> 
%% 		printVerboseLevel("~n~s Sender crashed: ~w, didn't send message:~w, msgId:~w ~n", 
%% 					  [node(),SenderInfo, Message, MsgId], 1);
	receiver_crashed -> 
		printVerboseLevel("~n~s Receiver crashed: ~w, didn't send Message: ~w~n", 
					  [node(),ReceiverInfo, Message], 1);
	_-> abort
end.

%%%
%%% Utilities Function to extract data from LastSentIdVector
%%%

getLastSentIdFor(ProcessInfo,LastSentIdVector) ->
 	case lists:keyfind(ProcessInfo,1,LastSentIdVector) of
		false-> 0;
		{_ProcessInfo, LastId} ->  LastId 
	end.

incrementLastSentId(ProcessInfo, LastSentIdVector) ->
	printVerboseLevel("~n>>> incrementLastSentId ProcessInfo: ~w ~n LastSentIdVector: ~w~n",[ProcessInfo, LastSentIdVector],8),
	case ProcessInfo of	{dummy}-> LastSentIdVector;
		_-> 
			case lists:keyfind(ProcessInfo,1,LastSentIdVector) of
				false-> LastSentIdVector ++ [{ProcessInfo, 1}];
				{_ProcessInfo, LastId} ->  lists:keyreplace(ProcessInfo,1,LastSentIdVector, 
													  {ProcessInfo, LastId+1}) 
			end
	end.

incrementAllLastSentId([], LastSentIdVector)->
	LastSentIdVector;
incrementAllLastSentId([{ReceiverInfo,_MatchingFunctions}|T], LastSentIdVector)->
	NewLastSentId = incrementLastSentId(ReceiverInfo, LastSentIdVector),
	incrementAllLastSentId(T, NewLastSentId).
	

setLastReceivedId(ProcessInfo, MsgId, LastReceivedIdVector)->
	case ProcessInfo of {dummy}-> LastReceivedIdVector;
	_-> 
		case lists:keyfind(ProcessInfo,1,LastReceivedIdVector) of
		false-> LastReceivedIdVector ++ [{ProcessInfo, MsgId}];
		{_ProcessInfo, LastId} ->  
			if MsgId>LastId ->
				   lists:keyreplace(ProcessInfo,1,LastReceivedIdVector,{ProcessInfo, MsgId});
				true-> LastReceivedIdVector
			end
	 	end
	end.

removeFromLastIdVector([], LastIdVector) ->
	LastIdVector;
removeFromLastIdVector([H|T], LastIdVector) ->
	NewIdVector = removeSingleProcessFromLastIdVector(H, LastIdVector),
	removeFromLastIdVector(T, NewIdVector).
	
removeSingleProcessFromLastIdVector(ProcessInfo, LastIdVector)->	
	case ProcessInfo of	{dummy}-> LastIdVector;
		_-> 
			case lists:keyfind(ProcessInfo,1,LastIdVector) of
				false-> LastIdVector;
				{ProcessInfo, LastId} ->  LastIdVector -- [{ProcessInfo, LastId}]
			end
	end.

%%%
%%% Utilities to deliver messages in causal order
%%%

returnMsgsToBeElaborated(Type, ReceivingQueue, LastIDVector)->
	returnMsgsToBeElaborated(Type, ReceivingQueue, ReceivingQueue, LastIDVector,[]).
returnMsgsToBeElaborated(_Type, ReceivingQueue, [], LastIDVector, Result)->
	{ReceivingQueue, LastIDVector, Result};
returnMsgsToBeElaborated(Type, ReceivingQueue, ToBeChecked, LastIDVector, Result) ->
	[Head|Rest] = ToBeChecked,
	case checkIfMsgCanBeDelivered(Type, Head, LastIDVector) of
		{_, false}-> 
			printVerboseLevel("~n~s@~s> Message is from the future: ~w~n", 
					 [string:to_upper(atom_to_list(Type)), node(), Head], 1),
			ackTheMessage(Type, Head, LastIDVector),
			returnMsgsToBeElaborated(Type, ReceivingQueue, Rest, LastIDVector, Result);
		{_,old}-> 
			printVerboseLevel("~n~s@~s> Message is old : ~w~n", 
					 [string:to_upper(atom_to_list(Type)), node(), Head], 1),
			ackTheMessage(Type, Head, LastIDVector),
			returnMsgsToBeElaborated(Type, ReceivingQueue -- [Head], Rest, LastIDVector, Result);
		{_, unexpected} ->
			printVerboseLevel("~n~s@~s> Message is unexpected: ~w~n", 
					 [string:to_upper(atom_to_list(Type)), node(), Head], 1),
			returnMsgsToBeElaborated(Type, ReceivingQueue -- [Head], Rest, LastIDVector, Result);
		{NewLastIDVector, true}  -> 
			printVerboseLevel("~n~s@~s> Message is ok: ~w~n", 
					 [string:to_upper(atom_to_list(Type)), node(), Head], 5),
			ackTheMessage(Type, Head, NewLastIDVector),
			returnMsgsToBeElaborated(Type, ReceivingQueue -- [Head],  ReceivingQueue -- [Head], 
									 NewLastIDVector, Result ++ [Head])
	end.

checkIfMsgCanBeDelivered(Type, Msg, LastIDVector) ->
	printVerboseLevel("~n~s@~s> Checking message: ~w, LastIDVector:~w~n", 
					 [string:to_upper(atom_to_list(Type)), node(), Msg, LastIDVector], 4),
	case Msg of 
		{_AckReplyInfo, _SenderInfo, _MsgID, die}->
			{LastIDVector, true};
		{_AckReplyInfo, SenderInfo, MsgID, _Content} ->
    		OldID = getLastSentIdFor(SenderInfo,LastIDVector),
    		NewIDVector =  setLastReceivedId(SenderInfo, MsgID, LastIDVector),
			if MsgID =:= OldID + 1 -> Value = true;
      			MsgID =< OldID -> Value = old;
       			true -> Value = false
   			end,
			{NewIDVector, Value};
		_-> {LastIDVector, unexpected}
end.

ackTheMessage(Type, Msg, LastIdVector)->
{AckPid, SenderInfo, MsgId,Content} = Msg,
case AckPid of
		{noreply} -> noreply;
		_-> 
			if is_pid(AckPid) ->				 
				 printVerboseLevel("~n~s@~s> Succesfully sent ACK to ~w, AckPid: ~w updated ID vector: ~w~n", 
					 [string:to_upper(atom_to_list(Type)), node(), SenderInfo, AckPid, LastIdVector], 5),
				AckPid! {ack_message, {Type, node()}, MsgId, Content};
			   true->
				printVerboseLevel("~n~s@~s> Could not sent ACK to ~w, AckPid is not a Pid: ~w~n", 
					 [string:to_upper(atom_to_list(Type)), node(), SenderInfo, LastIdVector], 2)
			end
end.


%% Check Father and if is dead, get adopted by dispatcher
checkFather(FatherInfo)->

%%% We first check if the node is supposed to have a father or not		
if FatherInfo =/= no_father ->	
case utility:remoteIsRegNameAlive(FatherInfo) of
	false -> 
		DispatcherInfo = {dispatcher,dispatcher:dispatcher_node()},	
		case FatherInfo of  
			DispatcherInfo->
			printVerboseLevel("BROKER@~s> Father is dispatcher and it is not alive:~w, broker dies~n", 
			 [node(), FatherInfo],1),
			erlang:exit(self(), normal),
			FatherInfo;
			_->
			NewFatherInfo = {dispatcher, dispatcher:dispatcher_node()},	
			case utility:remoteIsRegNameAlive(NewFatherInfo) of
				true->
					printVerboseLevel("BROKER@~s> Father Broker is not alive:~w, broker becomes direct child of dispatcher~n", 
					[node(), FatherInfo],1),
					NewFatherInfo;
				false-> 
					printVerboseLevel("BROKER@~s> Dispatcher is not alive, broker dies~n", 
			 		[node()],1),
					erlang:exit(self(), normal),
					FatherInfo
			end
		end;
	true -> FatherInfo;
	Any-> printVerboseLevel("~nBROKER@~s> Check Father State Exception: ~w ~n", [node(), Any ], 1), FatherInfo
end;
true ->
	FatherInfo
end.



%%%
%%% Small utilities for remote execution of predefined functions
%%%

%%% Remote is process alive
remoteIsProcessAlive(Pid) when is_pid(Pid) ->
    rpc:call(node(Pid), erlang, is_process_alive, [Pid]).

remotewhereis(Name, Node)->
	rpc:call(Node, erlang, whereis, [Name]).

remoteIsRegNameAlive({Name, Node})->
	remoteIsRegNameAlive(Name, Node);

remoteIsRegNameAlive(Any)->
	printVerboseLevel("~n>>> ERROR: Checking if remote process ~w is alive~n", [Any], 5).

remoteIsRegNameAlive(Name, Node) ->
	case remotewhereis(Name, Node) of 
		undefined-> Value = false;
		{badrpc,nodedown}-> Value =  false;
		Pid when is_pid(Pid)->  Value =  remoteIsProcessAlive(Pid);
		_Any -> Value = false
	end,
	printVerboseLevel("~n>>> Checking if remote process ~w on node ~w is alive, Value: ~w~n", [Name, Node, Value], 6),
	Value.


registerName(Name, Pid) ->
    case whereis(Name) of undefined->ok; _->unregister(Name) end, 
    register(Name, Pid),
	printVerboseLevel("~n***Registered name : ~w ~n ",[Name],7).


printVerboseLevel(Message, Args, Num)->
	if Num =< ?VERBOSE -> 
		io:format(Message, Args);
	   true-> ok
	end.