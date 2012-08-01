%% Author: Sara Magliacane
%% Created: 29/ago/2010
%% Description: TODO: Add description to broker
-module(broker).
%%
%% Include files
%% 
-import(utility, [registerName/2, unicastSend/4, multicastSend/4, incrementLastSentId/2,
				  getLastSentIdFor/2,
		  returnMsgsToBeElaborated/3, checkFather/1, printVerboseLevel/3, removeFromLastIdVector/2]).

-import(datautil, [computeDestinations/2, addInterest/3, addMoreInterests/3, deleteInterestAndRemoveEmpty/3,  
				   deleteInterest/3, deleteClient/2, deleteClients/2, extractAllMatchingFunctions/1, deleteMultipleInterest/3,
				   extractMatchingFunctions/2, printInterests/3, returnDeadProcessesInList/1]).
-import(errorhandler, [on_exit/5, sendStateToSupervisor/3, recoverBrokerSupervisor/4]).


%%
%% Exported Functions
%%
-export([start/0, start/1, start/2, init/1, brokerLoop/3,  stop/0, stop/1, 
		 printState/0,printState/1 ]).
% after 30 seconds without any msg he starts complaining ;)
% (diminuished for the sake of the debugging)
-define(inactivity_timeout, 30000). 
% used in the printVerboseLevel
-define(verbal_handicap, 0). 

%%
%% API Functions
%%

%% Start Broker on this node, remotely on node Node or attached to FatherNode

start()-> 
	start(node()).

%% Default Father = dispatcher
start(Node) -> 
	start(Node, []).

%% Start the Broker remotely on Node with State
start(Node, FatherNode) -> 
	if FatherNode == [] ->
		   	  Pid = spawn(Node,fun()-> init([]) end);
	   true->
		   %% check if Father is dead, in case print error message and become adopted by dispatcher
		   NewFatherInfo = checkFather({broker, FatherNode}),
		   Pid = spawn(Node,fun()-> init(NewFatherInfo) end)
	end,
    on_exit(Pid, Node, broker, broker, init).


%% Stop remotely Broker on Node - immediate
stop(Node)-> {broker, Node}!{{noreply},{dummy}, 1,{die}}.
stop() -> stop(node()).

%% Print state of Broker on Node -immediate
printState(Node)->{broker, Node}!{{noreply},{dummy}, 1,{printstate}}.
printState()-> printState(node()).

%%
%% Local Functions
%%


%%% Init function
init(State) ->
try
	case State of 
		[]-> 
			Type = broker, NewState = {[],[],[],[],[],[]}, 
			FatherInfo = {dispatcher, dispatcher:dispatcher_node()};
		{FatherName, FatherNode} -> 
			Type = broker, NewState = {[],[],[],[],[],[]},
			FatherInfo =  {FatherName, FatherNode};
		{Type, NewState, {FatherInfo, _OldPendingFatherInfo}}-> [] 
	end,
	
	registerName(Type, self()),
	%% in order to recover the error handler 
	process_flag(trap_exit, true),
		
	%% unroll state
	{ListOfClients, ListOfBrokers, ReceivingQueue, 
	LastDeliveredIdVector, LastSentIdVector, StillReadyMsgs} = NewState,
	printVerboseLevel("~nBROKER@~s> Broker started with State: ~w, FatherInfo:~w~n", [node(), NewState, FatherInfo], 3 +?verbal_handicap),
	
	%% If never contacted Father node (no LastSentId), just addbroker - 
	LastId = getLastSentIdFor(FatherInfo,LastSentIdVector),
	if LastId == 0 ->
		   NewLastSentIdVector =
			addBrokerToFather(Type, FatherInfo, ListOfClients, ListOfBrokers, LastSentIdVector),
		   PendingFatherInfo = FatherInfo,
		   %% marks the difference
		   TempFatherInfo = [];
	
	%% Else check Father and if is dead, get adopted by dispatcher
	   true-> 
		   TempFatherInfo = FatherInfo,
		   {PendingFatherInfo,NewLastSentIdVector} = 
		checkFatherAndAddBroker(Type, FatherInfo, ListOfClients, ListOfBrokers, LastSentIdVector)
	end,
	brokerLoop(Type, {ListOfClients, ListOfBrokers, ReceivingQueue, 
	LastDeliveredIdVector, NewLastSentIdVector, StillReadyMsgs}, {TempFatherInfo, PendingFatherInfo})
catch
	Any->
	  printVerboseLevel("~nBROKER@~s> Exception in init: ~w~nBROKER@~s> Broker restarted with state:~w~n",
				[node(),Any, node(), State],1 +?verbal_handicap),
	  init(State)
end.


%%% check Father broker and if it's dead, send state to new Father 
checkFatherAndAddBroker(Type, FatherInfo, ListOfClients, ListOfBrokers, LastSentIdVector)->	
	case Type of
		broker -> 
			NewFatherInfo = checkFather(FatherInfo),
			if NewFatherInfo =/= FatherInfo ->
				   	NewLastSentIdVector = 
						addBrokerToFather(Type, NewFatherInfo, ListOfClients, ListOfBrokers, LastSentIdVector),
					{NewFatherInfo,NewLastSentIdVector};
			   true-> {NewFatherInfo,LastSentIdVector}
			end;
		_ -> {FatherInfo,LastSentIdVector}
	end.
	

%%% send addbroker
addBrokerToFather(Type, FatherInfo, ListOfClients, ListOfBrokers, LastSentIdVector)->
	printVerboseLevel("~nBROKER@~s> Sent ADDBROKER to ~w~n", [node(), FatherInfo], 4 +?verbal_handicap),
	unicastSend({Type, node()}, {addbroker, {Type, node()}, {ListOfClients, ListOfBrokers}}, 
					FatherInfo, LastSentIdVector).


%%%
%%% Main Loop
%%%

brokerLoop(Type, State, {FatherInfo, PendingFatherInfo})->
try
	
	printVerboseLevel("~n~s@~s> Starting loop as ~w with State:~w, FatherInfo: ~w~n",  [string:to_upper(atom_to_list(Type)),node(), Type, State, FatherInfo], 6 +?verbal_handicap),
	
	%%% Unrolling state
	{OListOfClients, OListOfBrokers, OReceivingQueue, OLastDeliveredIdVector, _, OStillReadyMsgs} = State,
	
	
	%%% Receive loop
	receive 
		%% if the errorhandler dies respawn it	
		{'EXIT', _Pid, normal} -> 
			throw(exit),
			Msg =[], {AfterState, AfterFatherInfo} = {State, {FatherInfo, PendingFatherInfo}};
		{'EXIT', _Pid, Why} ->
			NLastSentIdVector = recoverBrokerSupervisor(Type, State, {FatherInfo, PendingFatherInfo}, Why),
			Msg = supervisor_crash, 
			{AfterState, AfterFatherInfo} = 
				{{OListOfClients, OListOfBrokers, OReceivingQueue, OLastDeliveredIdVector, NLastSentIdVector, OStillReadyMsgs}, {FatherInfo, PendingFatherInfo}};

		%% TODO sent empty messaged with MsgId, to unblock the queue
		{unicast_not_completed, ReceiverInfo, _MsgId, Message}-> 
			printVerboseLevel("~n~s ---- Unicast NOT completed for sender: ~w, receiver:~w, message: ~w~n", 
					  [node(),{Type, node()}, ReceiverInfo, Message], 1), 
			Msg =[], {AfterState, AfterFatherInfo} = {State, {FatherInfo, PendingFatherInfo}};
		{unicast_completed, ReceiverInfo, _MsgId, Message} -> 
			printVerboseLevel("~n~s ---- Unicast completed for sender: ~w, receiver:~w, message: ~w~n", 
					  [node(),{Type, node()}, ReceiverInfo, Message], 5), 
			Msg =[], {AfterState, AfterFatherInfo} = {State, {FatherInfo, PendingFatherInfo}};
		{multicast_completed,  ReceiversList, Message, _OldLastSentIdVector}->
			printVerboseLevel("~n~s ---- Multicast completed for sender: ~w, receiverslist:~w, message: ~w~n", 
					  [node(),{Type, node()}, ReceiversList, Message], 5), 
			Msg =[], {AfterState, AfterFatherInfo} = {State, {FatherInfo, PendingFatherInfo}};
		{multicast_not_completed, ReceiversList, Message, _OldLastSentIdVector}->
			printVerboseLevel("~n~s ---- Multicast NOT completed for sender: ~w, receiverslist:~w, message: ~w~n", 
					  [node(),{Type, node()}, ReceiversList, Message], 1),
			Msg =[], {AfterState, AfterFatherInfo} = {State, {FatherInfo, PendingFatherInfo}};


		Msg-> 
			printVerboseLevel("~n~s@~s> *** Received message: ~n~s@~s> *** ~w~n", [string:to_upper(atom_to_list(Type)), node(), string:to_upper(atom_to_list(Type)), node(), Msg], 4 +?verbal_handicap),
			{AfterState, AfterFatherInfo} = {State, {FatherInfo, PendingFatherInfo}}
	after ?inactivity_timeout->
		   {AfterState, AfterFatherInfo} =
			   handleInactivityTimeout(Type, State, {FatherInfo, PendingFatherInfo}), Msg = inactivity		
	end,
	
	%%% Unrolling state
	{ListOfClients, ListOfBrokers, ReceivingQueue, LastDeliveredIdVector, LastSentIdVector, StillReadyMsgs} = AfterState,
	
	case Msg of
		[]-> AReceivingQueue =  ReceivingQueue;
		supervisor_crash -> AReceivingQueue =  ReceivingQueue;
		inactivity ->  AReceivingQueue =  ReceivingQueue;
		_-> AReceivingQueue = ReceivingQueue ++ [Msg]
	end,

	%% Get all messages from the receiving queue which can be now delivered, 
	%% ack them and elaborate them
	{NewReceivingQueue, NewLastDeliveredIdVector,ReadyMsgs} = 
		returnMsgsToBeElaborated(Type, AReceivingQueue , LastDeliveredIdVector),
	
	printVerboseLevel("~n~s@~s> Delivered msgs: ~w;  Remaining msgs: ~w~n", 
					  [string:to_upper(atom_to_list(Type)), node(), StillReadyMsgs ++ ReadyMsgs, NewReceivingQueue], 5 +?verbal_handicap),
	
	
	%% Elaborate all ready msgs in order, send responses in separate processes
	%% and wait for the ACKs to discard them
	{NewListOfClients, NewListOfBrokers, NewLastSentIdVector, NewerLastDeliveredIdVector, 
	 {NewFatherInfo, NewPendingFatherInfo}, NewStillReadyMsgs} = 
		elaborateMsgs(Type, ListOfClients, ListOfBrokers, LastSentIdVector, 
					  NewLastDeliveredIdVector, AfterFatherInfo, StillReadyMsgs ++ ReadyMsgs),
	
	printVerboseLevel("~n~s@~s> Main Loop, after elaborate: NewListOfClients: ~w ; NewListOfBrokers: ~w, NewStillReadyMsgs: ~w~n", 
			  [string:to_upper(atom_to_list(Type)), node(), NewListOfClients, NewListOfBrokers, NewStillReadyMsgs], 5 +?verbal_handicap),
	

	case Msg of
		[]-> 
			NewState = {NewListOfClients, NewListOfBrokers, NewReceivingQueue, NewerLastDeliveredIdVector, NewLastSentIdVector, NewStillReadyMsgs};
		_-> 
			%% Send state to supervisor
			Supervisor = list_to_atom(string:concat("supervisor_", atom_to_list(Type))),
			NewerLastSentIdVector = incrementLastSentId({Supervisor, node()}, NewLastSentIdVector),
			NewState = {NewListOfClients, NewListOfBrokers, NewReceivingQueue, NewerLastDeliveredIdVector, NewerLastSentIdVector, NewStillReadyMsgs},
			sendStateToSupervisor(Type, {Type, NewState, {NewFatherInfo, NewPendingFatherInfo}}, NewLastSentIdVector)
	end,
		
	brokerLoop(Type, NewState, {NewFatherInfo, NewPendingFatherInfo})
catch
	exit -> unregister(Type),
			erlang:exit(self(), normal);
    Any->
	  printVerboseLevel("~n~s@~s> Exception: ~w~n~s@~s> Broker restarted with state:~w~n",
				[string:to_upper(atom_to_list(Type)), node(),Any, string:to_upper(atom_to_list(Type)), node(), State], 1 +?verbal_handicap),
	   brokerLoop(Type, State, {FatherInfo, PendingFatherInfo})
end.


%%%
%%%  ELABORATEMSGS
%%%
elaborateMsgs(Type, ListOfClients, ListOfBrokers, LastSentIdVector,  LastDeliveredIdVector, FatherInfo, ReadyMsgs) ->
	
	%% Update the list of clients and brokers, removing the dead ones, 
	%% except if we not waiting for the state of the father (could lead to inconsistency)
	{NLastSentIdVector, NLastDeliveredIdVector, NListOfClients, NListOfBrokers, BoolIsFatherDeadOrDisconnected} =
	updateBrokersAndClientsLists(Type, ListOfClients, ListOfBrokers, FatherInfo, LastSentIdVector, LastDeliveredIdVector),
	
	elaborateMsgs(Type, NListOfClients, NListOfBrokers, NLastSentIdVector, NLastDeliveredIdVector, FatherInfo, ReadyMsgs, [], BoolIsFatherDeadOrDisconnected).

elaborateMsgs(_Type, ListOfClients, ListOfBrokers, LastSentIdVector, LastDeliveredIdVector, FatherInfo, [],StillReadyMsgs, _BoolIsFatherDeadOrDisconnected) ->
    {ListOfClients, ListOfBrokers, LastSentIdVector, LastDeliveredIdVector, FatherInfo, StillReadyMsgs};

elaborateMsgs(Type, ListOfClients, ListOfBrokers, LastSentIdVector, LastDeliveredIdVector, FatherInfo, ReadyMsgs, StillReadyMsgs, BoolIsFatherDeadOrDisconnected) ->
    
	printVerboseLevel("~n~w@~w> ListOfClients ~w, ListOfBrokers ~w, LastSentIdVector ~w, LastDeliveredIdVector ~w, FatherInfo ~w, ReadyMsgs ~w, StillReadyMsgs ~w ~n",
		[Type, node(), ListOfClients, ListOfBrokers, LastSentIdVector, LastDeliveredIdVector, FatherInfo, ReadyMsgs, StillReadyMsgs], 5),
    
	[FirstMsg| Rest] = ReadyMsgs,
    	
	%%% Messages which are elaborated anyway
	%%% both if the father is dead or not
	{NewListOfBrokers, NewListOfClients,  {NewLastSentIdVector,NewLastDeliveredIdVector}, 
	 	NewFatherInfo, NewRest, NewStillReadyMsgs, NewFirstMsg, NewBoolIsFatherDeadOrDisconnected} =
		elaborationOfSpecialMessages(FirstMsg, Type, ListOfClients, ListOfBrokers, LastSentIdVector, 
							   LastDeliveredIdVector, FatherInfo, Rest, StillReadyMsgs, BoolIsFatherDeadOrDisconnected),
	
	%%% Messages elaborated only if Father, otherwise they become part of the StillReady queue
	case NewBoolIsFatherDeadOrDisconnected of
	
	true-> 
	%% Father is dead or disconnected - send add broker or just wait for its state
		{OldFatherInfo, PendingFatherInfo} = NewFatherInfo,
		
		if OldFatherInfo =/= PendingFatherInfo ->
		%% we already sent an addbroker, waiting for Father's state
			  {NewPendingFatherInfo,NewerLastSentIdVector} = {PendingFatherInfo,LastSentIdVector};
		true ->	   
			{NewPendingFatherInfo,NewerLastSentIdVector} = 
			checkFatherAndAddBroker(Type, OldFatherInfo, NewListOfClients, NewListOfBrokers, NewLastSentIdVector)
   		end,
		elaborateMsgs(Type, NewListOfClients, NewListOfBrokers, NewerLastSentIdVector, NewLastDeliveredIdVector, 
			  {OldFatherInfo, NewPendingFatherInfo}, NewRest, NewStillReadyMsgs ++ [NewFirstMsg], NewBoolIsFatherDeadOrDisconnected);		
	false->
  	%%% Father is not dead, we elaborated messages normally
		normalElaboratingOfMsg(NewFirstMsg, Type, NewListOfClients, NewListOfBrokers, NewLastSentIdVector, 
				NewLastDeliveredIdVector, NewFatherInfo, NewRest, NewStillReadyMsgs, NewBoolIsFatherDeadOrDisconnected)
end.		
	
elaborationOfSpecialMessages(FirstMsg, Type, ListOfClients, ListOfBrokers, LastSentIdVector, 
							   LastDeliveredIdVector, {FatherInfo, PendingFatherInfo}, Rest, StillReadyMsgs, 
							 BoolIsFatherDeadOrDisconnected)->
{_AckReplyInfo, _SenderInfo, _MsgId, Content} = FirstMsg,
case Content of
	
		{state, PendingFatherInfo, ListOfMatchingFunctions}->
     		%% the new father is "alive"
			printVerboseLevel("~n~s@~s> Received Father: ~w, State: ~w~n", 
				[string:to_upper(atom_to_list(Type)), node(), PendingFatherInfo, ListOfMatchingFunctions], 4 +?verbal_handicap),
			
			
			%% add father to broker nodes
			case PendingFatherInfo of
				no_father ->
					NListOfBrokers = ListOfBrokers;
				_-> 
					NListOfBrokers = 
						addMoreInterests(ListOfBrokers, PendingFatherInfo,ListOfMatchingFunctions)
			end,
			
			%% we need to elaborate all StillReady messages which we froze during NewFather change 
			{NListOfBrokers, ListOfClients,  {LastSentIdVector,LastDeliveredIdVector},
			  {PendingFatherInfo, PendingFatherInfo}, StillReadyMsgs ++ Rest, [], {[],[],[],[already_elaborated]}, false};
		
		
      	{die} ->
	  		NewLastSentIdVector = killBroker(Type, node(), ListOfBrokers, deleteClient(ListOfBrokers, {Type, node()}), 
											  {FatherInfo, PendingFatherInfo}, LastSentIdVector),
	  		throw(exit),
			{ListOfBrokers, ListOfClients, {NewLastSentIdVector,LastDeliveredIdVector},
			 {FatherInfo, PendingFatherInfo}, Rest, StillReadyMsgs, {[],[],[],[already_elaborated]}, BoolIsFatherDeadOrDisconnected};
      
      	{printstate} ->
	  		datautil:printInterests(Type, ListOfClients, ListOfBrokers),
	  		{ListOfBrokers, ListOfClients, {LastSentIdVector,LastDeliveredIdVector}, 
			     {FatherInfo, PendingFatherInfo}, Rest, StillReadyMsgs, {[],[],[],[already_elaborated]}, BoolIsFatherDeadOrDisconnected};
		_-> {ListOfBrokers, ListOfClients, {LastSentIdVector,LastDeliveredIdVector},
			 {FatherInfo, PendingFatherInfo}, Rest, StillReadyMsgs, FirstMsg, BoolIsFatherDeadOrDisconnected}
end.


normalElaboratingOfMsg(FirstMsg, Type, ListOfClients, ListOfBrokers, LastSentIdVector, 
					   LastDeliveredIdVector, FatherInfo, Rest, StillReadyMsgs, BoolIsFatherDeadOrDisconnected)->
{_AckReplyInfo, _SenderInfo, _MsgId, Content} = FirstMsg,
	
	printVerboseLevel("~n~s@~s> Elaborating msg: ~w~n",
				[string:to_upper(atom_to_list(Type)), node(), FirstMsg], 5 +?verbal_handicap),
	case Content of
      {subscribe, ClientNode, MatchingFunction} ->
	  	NewLastSentIdVector = 
			subscriptionForwarding({client,ClientNode}, MatchingFunction, Type,
								ListOfBrokers, LastSentIdVector),
	  	elaborateMsgs(Type, addInterest(ListOfClients, {client, ClientNode}, MatchingFunction),
			ListOfBrokers, NewLastSentIdVector, LastDeliveredIdVector, FatherInfo, Rest,StillReadyMsgs, BoolIsFatherDeadOrDisconnected);
		
	  {subscription_forward, BrokerInfo, MatchingFunction}->
		  NewLastSentIdVector = 
			  subscriptionForwarding(BrokerInfo, MatchingFunction, Type,
								deleteClient(ListOfBrokers, BrokerInfo), LastSentIdVector),
		  elaborateMsgs(Type, ListOfClients, addInterest(ListOfBrokers, BrokerInfo, MatchingFunction),
		     NewLastSentIdVector, LastDeliveredIdVector, FatherInfo, Rest, StillReadyMsgs, BoolIsFatherDeadOrDisconnected);
		
	  {multiple_sub_forward, BrokerInfo, ReceivedMatchingFunctions}->
		 NewLastSentIdVector = 
			  multipleSubscriptionsForwarding(BrokerInfo, ReceivedMatchingFunctions, Type,
								deleteClient(ListOfBrokers, BrokerInfo), LastSentIdVector),
		  	elaborateMsgs(Type, ListOfClients, addMoreInterests(ListOfBrokers, BrokerInfo, ReceivedMatchingFunctions),
		      NewLastSentIdVector, LastDeliveredIdVector, FatherInfo, Rest, StillReadyMsgs, BoolIsFatherDeadOrDisconnected);	
      
      {unsubscribe, ClientNode, MatchingFunction} ->
	  	NewLastSentIdVector =  
			unsubscriptionForwarding({client, ClientNode}, MatchingFunction,
									    Type, ListOfBrokers, LastSentIdVector),
	  	elaborateMsgs(Type, deleteInterestAndRemoveEmpty(ListOfClients, {client, ClientNode}, MatchingFunction),
			ListOfBrokers, NewLastSentIdVector, LastDeliveredIdVector, FatherInfo, Rest, StillReadyMsgs, BoolIsFatherDeadOrDisconnected);
		
	  {unsubscription_forward, BrokerInfo, MatchingFunction}->
		  NewLastSentIdVector =  
			  unsubscriptionForwarding(BrokerInfo, MatchingFunction,Type, 
										deleteClient(ListOfBrokers, BrokerInfo), LastSentIdVector),
	  	elaborateMsgs(Type, ListOfClients , deleteInterest(ListOfBrokers, BrokerInfo, MatchingFunction),
			NewLastSentIdVector, LastDeliveredIdVector, FatherInfo, Rest, StillReadyMsgs, BoolIsFatherDeadOrDisconnected);
		
	  {multiple_unsub_forward, BrokerInfo, ListOfMatchingFunctions}->
			 NewLastSentIdVector =  
			  multipleUnsubscriptionForwarding(BrokerInfo, ListOfMatchingFunctions,Type, 
										deleteClient(ListOfBrokers, BrokerInfo), LastSentIdVector),
			 elaborateMsgs(Type, ListOfClients , deleteMultipleInterest(ListOfBrokers, BrokerInfo, ListOfMatchingFunctions),
			NewLastSentIdVector, LastDeliveredIdVector, FatherInfo, Rest, StillReadyMsgs, BoolIsFatherDeadOrDisconnected);
      
		
      {publish, ClientNode, Message} ->
	  	NewLastSentIdVector = 
			publishForwarding({client, ClientNode}, Message, Type, 
							ListOfClients, ListOfBrokers, LastSentIdVector),
	  	elaborateMsgs(Type, ListOfClients, ListOfBrokers,NewLastSentIdVector, LastDeliveredIdVector, 
					  FatherInfo, Rest, StillReadyMsgs, BoolIsFatherDeadOrDisconnected);
		
	  {publication_forward, BrokerInfo, Message}->
		 NewLastSentIdVector = 
			 publishForwarding(BrokerInfo, Message, Type, ListOfClients, 
							deleteClient(ListOfBrokers, BrokerInfo), LastSentIdVector),
	  	elaborateMsgs(Type, ListOfClients, ListOfBrokers,NewLastSentIdVector, LastDeliveredIdVector, 
					  FatherInfo, Rest, StillReadyMsgs, BoolIsFatherDeadOrDisconnected);
      
      {addbroker, BrokerInfo, {ReceivedListOfBrokers, ReceivedListOfClients}} ->
		{NewLastSentIdVector, ReceivedMatchingFunctions} = 
			additionForwarding(BrokerInfo, Type, ListOfBrokers,
						   ListOfClients, ReceivedListOfClients ++ ReceivedListOfBrokers, LastSentIdVector),
		elaborateMsgs(Type, ListOfClients, addMoreInterests(ListOfBrokers,
			BrokerInfo, ReceivedMatchingFunctions), NewLastSentIdVector, LastDeliveredIdVector, FatherInfo, Rest, StillReadyMsgs,BoolIsFatherDeadOrDisconnected);
      
      {removebroker, BrokerName, BrokerNode} ->
	  	NewLastSentIdVector = 
			removalForwarding({BrokerName,BrokerNode}, Type, ListOfBrokers, deleteClient(ListOfBrokers, BrokerNode),
							   LastSentIdVector),
	  	elaborateMsgs(Type, ListOfClients, deleteClient(ListOfBrokers, BrokerNode),
			NewLastSentIdVector,LastDeliveredIdVector, FatherInfo, Rest, StillReadyMsgs, BoolIsFatherDeadOrDisconnected);
		
		_Any -> 
		elaborateMsgs(Type, ListOfClients, ListOfBrokers,
			LastSentIdVector,LastDeliveredIdVector, FatherInfo, Rest, StillReadyMsgs, BoolIsFatherDeadOrDisconnected)
	end.

%%%
%%% Utility functions in the elaborateMsgs loop
%%%

subscriptionForwarding(_BrokerInfo, [], _Type, _ListOfBrokers, LastSentIdVector) ->
	LastSentIdVector;
subscriptionForwarding(BrokerInfo, MatchingFunction, Type, [], LastSentIdVector) ->
	printVerboseLevel("~n~s@~s Received SUBSCRIBE from ~w with ~w~n",
					  [string:to_upper(atom_to_list(Type)), node(), BrokerInfo, MatchingFunction],2 +?verbal_handicap),
	LastSentIdVector;
subscriptionForwarding(BrokerInfo, MatchingFunction, Type, ListOfBrokers, LastSentIdVector) ->
	printVerboseLevel("~n~s@~s Received SUBSCRIBE from ~w with ~w~n",
					  [string:to_upper(atom_to_list(Type)), node(), BrokerInfo, MatchingFunction],2 +?verbal_handicap),
	
	%% Forward to all brokers in the list (includes the father)
	printVerboseLevel("~n~s@~s Forwarding SUBSCRIBE to Brokers: ~w  with: ~w ~n", 
					   [string:to_upper(atom_to_list(Type)), node(), ListOfBrokers, MatchingFunction],2 +?verbal_handicap),
	
	multicastSend({Type, node()}, {subscription_forward, {Type, node()}, MatchingFunction}, ListOfBrokers,  
				  LastSentIdVector).

unsubscriptionForwarding(_BrokerInfo, [], _Type, _ListOfBrokers, LastSentIdVector) ->
	LastSentIdVector;
unsubscriptionForwarding(BrokerInfo, MatchingFunction, Type, [], LastSentIdVector) ->
	printVerboseLevel("~n~s@~s Received UNSUBSCRIBE from ~w with ~w~n", 
					  [string:to_upper(atom_to_list(Type)), node(), BrokerInfo, MatchingFunction],2 +?verbal_handicap),
	LastSentIdVector;
unsubscriptionForwarding(ClientInfo, MatchingFunction, Type , ListOfBrokers, LastSentIdVector) ->
    printVerboseLevel("~n~s@~s Received UNSUBSCRIBE from ~w with ~w~n", 
					  [string:to_upper(atom_to_list(Type)), node(), ClientInfo, MatchingFunction],2 +?verbal_handicap),

	%% Forward to all brokers in the list (includes the father)
	printVerboseLevel("~n~s@~s Forwarding UNSSUBSCRIBE to Brokers: ~w  with: ~w ~n", 
					   [string:to_upper(atom_to_list(Type)), node(), ListOfBrokers, MatchingFunction],2 +?verbal_handicap),
	multicastSend({Type, node()}, {unsubscription_forward, {Type, node()}, MatchingFunction}, ListOfBrokers, 
				  LastSentIdVector). 

publishForwarding(_ClientInfo,[], _Type, _ListOfClients, _ListOfBrokers, LastSentIdVector)->
	LastSentIdVector;
publishForwarding(ClientInfo, Message, Type, [], [], LastSentIdVector)->
	printVerboseLevel("~n~s@~s Received PUBLISH from ~w: '~s' ~n", 
					   [string:to_upper(atom_to_list(Type)), node(), ClientInfo, Message],2 +?verbal_handicap),
	LastSentIdVector;
publishForwarding(ClientInfo,Message, Type, ListOfClients, ListOfBrokers, LastSentIdVector) ->
    printVerboseLevel("~n~s@~s Received PUBLISH from ~w: '~s' ~n", 
					   [string:to_upper(atom_to_list(Type)), node(), ClientInfo, Message],2 +?verbal_handicap),
	
	%% Check alive Brokers and calculate to which we want to forward the publication
	BrokerDestinations = computeDestinations(Message, ListOfBrokers),
	ClientDestinations = computeDestinations(Message, ListOfClients),
	
	printVerboseLevel("~n~s@~s Forwarding PUBLISH to Clients: ~w  and Brokers: ~w ~n", 
					   [string:to_upper(atom_to_list(Type)), node(), ClientDestinations, BrokerDestinations],2 +?verbal_handicap),
	multicastSend({Type, node()}, {publication_forward, {Type, node()}, Message}, BrokerDestinations ++ ClientDestinations, LastSentIdVector).

%%% Make multiple subscriptions
%%% Uses subscriptionForwarding
multipleSubscriptionsForwarding(_BrokerInfo, [], _Type,  _ListOfBrokers, LastSentIdVector)->
	LastSentIdVector;
multipleSubscriptionsForwarding(BrokerInfo, ReceivedMatchingFunctions, Type,  [], LastSentIdVector)->
	printVerboseLevel("~n~s@~s Received MULTIPLE_SUBSCRIBE from ~w with ~w~n", 
					  [string:to_upper(atom_to_list(Type)), node(), BrokerInfo, ReceivedMatchingFunctions],2 +?verbal_handicap),
	LastSentIdVector;
multipleSubscriptionsForwarding(BrokerInfo, ReceivedMatchingFunctions, Type,  ListOfBrokers, LastSentIdVector)->
	printVerboseLevel("~n~s@~s Received MULTIPLE_SUBSCRIBE from ~w with ~w~n", 
					  [string:to_upper(atom_to_list(Type)), node(), BrokerInfo, ReceivedMatchingFunctions],2 +?verbal_handicap),
	
	%% Forward to all brokers in the list (includes the father)
	printVerboseLevel("~n~s@~s Forwarding MULTIPLE_SUBSCRIBE to Brokers: ~w  with: ~w ~n", 
					   [string:to_upper(atom_to_list(Type)), node(), ListOfBrokers, ReceivedMatchingFunctions],2 +?verbal_handicap),
	multicastSend({Type, node()}, {multiple_sub_forward, {Type, node()}, ReceivedMatchingFunctions}, ListOfBrokers, 
				  LastSentIdVector).


multipleUnsubscriptionForwarding(_BrokerInfo, [],_Type, _ListOfBrokers, LastSentIdVector)->
	LastSentIdVector;
multipleUnsubscriptionForwarding(BrokerInfo, ListOfMatchingFunctions,Type, [], LastSentIdVector)->
	printVerboseLevel("~n~s@~s Received MULTIPLE_UNSUBSCRIBE from ~w with ~w~n", 
					  [string:to_upper(atom_to_list(Type)), node(), BrokerInfo, ListOfMatchingFunctions],2 +?verbal_handicap),
	LastSentIdVector;
multipleUnsubscriptionForwarding(BrokerInfo, ListOfMatchingFunctions,Type, ListOfBrokers, LastSentIdVector)->
	printVerboseLevel("~n~s@~s Received MULTIPLE_UNSUBSCRIBE from ~w with ~w~n", 
					  [string:to_upper(atom_to_list(Type)), node(), BrokerInfo, ListOfMatchingFunctions],2 +?verbal_handicap),

	%% Forward to all brokers in the list (includes the father)
	printVerboseLevel("~n~s@~s Forwarding MULTIPLE_UNSUBSCRIBE to Brokers: ~w  with: ~w ~n", 
					   [string:to_upper(atom_to_list(Type)), node(), ListOfBrokers, ListOfMatchingFunctions],2 +?verbal_handicap),
	multicastSend({Type, node()}, {multiple_unsub_forward, {Type, node()}, ListOfMatchingFunctions}, ListOfBrokers, 
				  LastSentIdVector).


%%% Deal with removebroker, remove recursively the broker, by sending unsubscribe for all his matching functions
%%% to all other brokers
removalForwarding(BrokerInfo,Type, _OldListOfBrokers,  [], LastSentIdVector)->
	printVerboseLevel("~n~s@~s Received REMOVEBROKER from ~w~n", 
					   [string:to_upper(atom_to_list(Type)), node(), BrokerInfo],2 +?verbal_handicap),
	LastSentIdVector;
removalForwarding(BrokerInfo,Type, OldListOfBrokers,  ListOfBrokers, LastSentIdVector)->
	printVerboseLevel("~n~s@~s Received REMOVEBROKER from ~w~n", 
					   [string:to_upper(atom_to_list(Type)), node(), BrokerInfo],2 +?verbal_handicap),
	
	%% unsubscribe for all removed Broker Matching Functions
	ListOfMatchingFunctions = extractMatchingFunctions(OldListOfBrokers, BrokerInfo),
	printVerboseLevel("~n~s@~s Forwarding MULTIPLE_UNSUBSCRIBE to Brokers: ~w  with: ~w ~n", 
					   [string:to_upper(atom_to_list(Type)), node(), ListOfBrokers, ListOfMatchingFunctions],2 +?verbal_handicap),
	
	multicastSend({Type, node()}, {multiple_unsub_forward, {Type, node()}, ListOfMatchingFunctions}, ListOfBrokers, 
				  LastSentIdVector).
	

killBroker(Type, Node, OldListOfBrokers, ListOfBrokers, FatherInfo, LastSentIdVector)->
	if Type == dispatcher -> 
		removalForwarding({Type, node()},Type, OldListOfBrokers, ListOfBrokers, LastSentIdVector);
	   true->
	%%% I send removebroker to my father which will manage the removal forwarding
		{Father, _PendingFather} = FatherInfo,
	 	utility:unicastSendWithoutAck({Type, node()}, {removebroker, Type, Node}, Father, LastSentIdVector)
	end.


%%% Deal with ADDBroker message, forward all subscriptions of the added broker
%%% Send back the ListOfMatchingFunctions of the Father
%%% Uses multipleSubscriptionsForwarding and removalForwarding
additionForwarding(BrokerInfo, Type, ListOfBrokers, ListOfClients,  
				   ReceivedListOfChildren, LastSentIdVector)->
	printVerboseLevel("~n~s@~s Received ADDBROKER from ~w ReceivedListOfChildren ~w~n",
					  [string:to_upper(atom_to_list(Type)), node(), BrokerInfo, ReceivedListOfChildren],2 +?verbal_handicap),
	
	%%% Forward state to added broker
	State = extractAllMatchingFunctions(ListOfClients ++ ListOfBrokers),
	%%% Hopefully it is alive
	case utility:remoteIsRegNameAlive(BrokerInfo) of
	true->
		% need to forward addition
		ReceivedMatchingFunctions = extractAllMatchingFunctions(ReceivedListOfChildren),
		NewLastSentIdVector = 
			multipleSubscriptionsForwarding(BrokerInfo, ReceivedMatchingFunctions, Type, 
										deleteClient(ListOfBrokers, BrokerInfo), LastSentIdVector),
		NewerLastSentIdVector = unicastSend({Type, node()}, {state, {Type, node()}, State}, 
						BrokerInfo, NewLastSentIdVector),
		NewReceivedMatchingFunctions = ReceivedMatchingFunctions;
	false->
		
		NewerLastSentIdVector = LastSentIdVector,
		NewReceivedMatchingFunctions = []
	end,
	{NewerLastSentIdVector, NewReceivedMatchingFunctions}.


multipleProcessesRemoval(DeadBrokers, DeadClients, Type, ListOfClients, OldListOfBrokers, ListOfBrokers, LastSentIdVector)->
	multipleBrokerRemoval(DeadBrokers, DeadClients, Type, ListOfClients, OldListOfBrokers, ListOfBrokers, LastSentIdVector, []).


multipleBrokerRemoval([], DeadClients, Type, ListOfClients, _OldListOfBrokers, ListOfBrokers, LastSentIdVector, FunctionsToBeRemoved) ->
    multipleClientsRemoval(DeadClients, Type, ListOfClients, ListOfBrokers, LastSentIdVector,FunctionsToBeRemoved);
multipleBrokerRemoval([Head| RestOfBrokersToBeRemoved], DeadClients, Type, ListOfClients, OldListOfBrokers, ListOfBrokers, LastSentIdVector, FunctionsToBeRemoved) ->
    printVerboseLevel("~n~s@~s Removing dead broker ~w~n",
		      [string:to_upper(atom_to_list(Type)), node(), Head], 2 + ?verbal_handicap),
	ListOfMatchingFunctions = extractMatchingFunctions(OldListOfBrokers, Head),
	multipleBrokerRemoval(RestOfBrokersToBeRemoved, DeadClients, Type,ListOfClients,  OldListOfBrokers, ListOfBrokers, LastSentIdVector, FunctionsToBeRemoved ++ ListOfMatchingFunctions ).


multipleClientsRemoval([], _Type, _ListOfClients, _ListOfBrokers, LastSentIdVector, [])->
	LastSentIdVector;
multipleClientsRemoval([], Type, _ListOfClients, ListOfBrokers, LastSentIdVector, FunctionsToBeRemoved) ->
     multicastSend({Type, node()}, {multiple_unsub_forward, {Type, node()}, FunctionsToBeRemoved}, ListOfBrokers, 
				  LastSentIdVector);
multipleClientsRemoval([Head| RestOfClientsToBeRemoved], Type, ListOfClients, ListOfBrokers, LastSentIdVector,FunctionsToBeRemoved) ->
    printVerboseLevel("~n~s@~s Removing dead client ~w~n",
		      [string:to_upper(atom_to_list(Type)), node(), Head], 2 + ?verbal_handicap),
   	ListOfMatchingFunctions = extractMatchingFunctions(ListOfClients, Head),
    multipleClientsRemoval(RestOfClientsToBeRemoved, Type, ListOfClients, ListOfBrokers, LastSentIdVector, FunctionsToBeRemoved ++ ListOfMatchingFunctions).


updateBrokersAndClientsLists(Type, ListOfClients, ListOfBrokers, {FatherInfo, Pend}, LastSentIdVector, LastDeliveredIdVector) ->
    %% Check for dead brokers in the list and remove them
    DeadBrokers = returnDeadProcessesInList(ListOfBrokers),
	DeadClients = returnDeadProcessesInList(ListOfClients),

	 %% Is Father dead or disconnected?
	BoolIsFatherDeadOrDisconnected = (lists:member(FatherInfo, DeadBrokers)) or (FatherInfo =/= Pend),
    
	%% if father is dead or disconnected, we wait to execute the removal in order to have
	%% a consistent state (because we sent a state to the father in which we were subscribed 
	%% to this matching functions) - we will do it anyway when we receive the state
	case BoolIsFatherDeadOrDisconnected of
	false->
		NewListOfBrokers = deleteClients(ListOfBrokers, DeadBrokers),
		NewListOfClients = deleteClients(ListOfClients, DeadClients),
		printVerboseLevel("~n~s@~s Epurated ListOfBrokers:~w, DeadBrokers:~w, ListOfClients:~w~n",
		      [string:to_upper(atom_to_list(Type)), node(), NewListOfBrokers, DeadBrokers, NewListOfClients], 5 + ?verbal_handicap),
	    
		%% Modify LastSentIdVector and LastDeliveredIdVector, so if another broker gets spawned
		%% on the same node, he can communicate with the broker
		NewLastDeliveredIdVector = removeFromLastIdVector(DeadBrokers, LastDeliveredIdVector),
		NewLastSentIdVector = removeFromLastIdVector(DeadBrokers, LastSentIdVector),
		NewerLastDeliveredIdVector = removeFromLastIdVector(DeadClients, NewLastDeliveredIdVector),
		NewerLastSentIdVector = removeFromLastIdVector(DeadClients, NewLastSentIdVector),
		printVerboseLevel("~n~s@~s NewLastDeliveredIdVector:~w, NewLastSentIdVector:~w~n",
		      [string:to_upper(atom_to_list(Type)), node(), NewerLastDeliveredIdVector, NewerLastSentIdVector], 8+ ?verbal_handicap),
	
		%% Forward unsubscription to all their matching functions to the alive brokers
   	 	NewestLastSentIdVector =
			multipleProcessesRemoval(DeadBrokers, DeadClients, Type,  ListOfClients, ListOfBrokers, NewListOfBrokers, NewerLastSentIdVector),
		{NewestLastSentIdVector, NewerLastDeliveredIdVector,  NewListOfClients, NewListOfBrokers, BoolIsFatherDeadOrDisconnected};
	%% father dead or disconnected
	true ->	
		{LastSentIdVector, LastDeliveredIdVector, ListOfClients,  ListOfBrokers, BoolIsFatherDeadOrDisconnected}
	end.	

%%% callled in the main loop after ?inactivity_timeout
handleInactivityTimeout(Type, State, {FatherInfo, PendingFatherInfo})->
%%% Update lists by removing dead brokers and clients
%%% Restart LastSentIdVector and LastDeliveredIdVector for removed nodes
%%% so in case they regenerate they can start again

%%% Unrolling state
	{ListOfClients, ListOfBrokers, ReceivingQueue, LastDeliveredIdVector, LastSentIdVector, StillReadyMsgs} = State,
	

%%% Update list of brokers and clients by cleaning dead processes	
	{NLastSentIdVector, NLastDeliveredIdVector, NListOfClients, NListOfBrokers,BoolIsFatherDeadOrDisconnected} = 	
	updateBrokersAndClientsLists(Type, ListOfClients, ListOfBrokers, {FatherInfo, PendingFatherInfo}, 
								 LastSentIdVector, LastDeliveredIdVector),
		
		
%% check Father broker and if it's dead (not just disconnected) send addbroker to new Father
if (PendingFatherInfo == FatherInfo) and (BoolIsFatherDeadOrDisconnected == true) ->
		{NewPendingFatherInfo, NNLastSentIdVector} = 
			checkFatherAndAddBroker(Type, FatherInfo, NListOfClients, NListOfBrokers, NLastSentIdVector);
%% Father is not dead or we are already waiting for the state of the new one		
   true-> {NewPendingFatherInfo,NNLastSentIdVector} = {PendingFatherInfo,NLastSentIdVector}
   end,
	
   {{NListOfClients, NListOfBrokers, ReceivingQueue, NLastDeliveredIdVector, NNLastSentIdVector, StillReadyMsgs}, {FatherInfo, NewPendingFatherInfo}}.