%% Author: Sara Magliacane
%% Created: 29/ago/2010
%% Description: TODO: Add description to client
-module(client).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([start/0, start/1, start/2, init/1, initnext/1,
		subscribe/2,subscribe/3, unsubscribe/2,unsubscribe/3, publish/2, publish/3]). 
-export([next/1, getNextId/1]).
-import(utility, [returnMsgsToBeElaborated/3,  printVerboseLevel/3, incrementLastSentId/2]).
-import(utility, [remoteIsProcessAlive/1, remotewhereis/2, registerName/2,
	 remoteIsRegNameAlive/2, unicastSend/4, multicastSend/4]).
-import(datautil, [computeDestinations/2, addInterest/3, deleteInterest/3, deleteClient/2,
	 extractAllMatchingFunctions/1, extractMatchingFunctions/2, printInterests/1]).
-import(errorhandler, [on_exit/5, sendStateToSupervisor/3, recoverClientSupervisor/2]).
% after 2 minutes without any msg the client starts complaining ;)
-define(inactivity_timeout, 120000). 
% used in the printVerboseLevel
-define(verbal_handicap, 0). 

%%
%% API Functions
%%
start()-> start(node()).
start(ClientNode) -> 
	start(ClientNode, []).
	
start(ClientNode, CallbackFunction) ->
	%% used to have the correct MsgId for the shell commands (ordering of messages)
	initnext(ClientNode),
	State = [[],[],[],[], CallbackFunction],
	Pid = spawn(ClientNode,fun()-> init(State) end),
	%% start errorhandler - on exit execute client:init
    on_exit(Pid, ClientNode, client, client, init).


subscribe(BrokerNode, MatchingFunction) ->
    subscribe(node(), BrokerNode, MatchingFunction).
subscribe(ClientNode,BrokerNode,MatchingFunction)->
	%% start client if it wasn't already alive
	printVerboseLevel("~nSHELL> Sent SUBSCRIBE_REQ to client: ~w~n", [ClientNode],4),
	case remoteIsRegNameAlive(client, ClientNode) of false ->  
					start(ClientNode),
					timer:sleep(50); 
		 true-> ok 
	end,
	%% send the subscribe request - noreply means we need no ack
	{client, ClientNode}! {{noreply},{client, ClientNode}, getNextId(ClientNode),
						   {subscribe_req, {broker, BrokerNode}, MatchingFunction}}.
	
unsubscribe(BrokerNode, MatchingFunction)->
	unsubscribe(node(),BrokerNode, MatchingFunction).
unsubscribe(ClientNode,BrokerNode,MatchingFunction)->
	printVerboseLevel("~nSHELL> Sent UNSUBSCRIBE_REQ to client: ~w~n", [ClientNode],4),
	case remoteIsRegNameAlive(client, ClientNode) of 
		false -> 
			printVerboseLevel("~nCLIENT@~s> EXCEPTION: client not alive~n ",
				[node()],1 + ?verbal_handicap);
		true-> 
		%% send the unsubscribe request - noreply means we need no ack
		{client, ClientNode}! {{noreply},{client, ClientNode}, getNextId(ClientNode),
						   {unsubscribe_req, {broker, BrokerNode}, MatchingFunction}},[]
	end.

publish(BrokerNode, Message) ->
	publish(node(), BrokerNode, Message).
publish(ClientNode, BrokerNode, Message)->
	printVerboseLevel("~nSHELL> Sent PUBLISH_REQ to client: ~w~n", [ClientNode],4),
	case remoteIsRegNameAlive(client, ClientNode) of false ->  
				start(ClientNode),
				timer:sleep(50); 
		true -> ok 
	end,
	%% send the publish request - noreply means we need no ack
    {client, ClientNode}! {{noreply},{client, ClientNode}, getNextId(ClientNode),
						   {publish_req,{broker, BrokerNode}, Message}},[].

%%
%% Local Functions
%%

init(State) -> 
	printVerboseLevel("~nCLIENT@~s> Registered name client~n", [node()], 4 + ?verbal_handicap),
	registerName(client, self()),
	%% in order to recover the error handler
	process_flag(trap_exit, true),
	
	if State == [] -> NewState = [[],[],[],[],[]];
	   true-> NewState = State
	end,
		
	clientLoop(NewState).

%%% Main Loop, similar to Broker's
clientLoop(State) ->
try
	[Subscription, ReceivingQueue, LastDeliveredIdVector, LastSentIdVector, CallbackFunction] = State,
	
	%% printClientState(node(), State),
	%% Receive loop
	receive 
		%% if the errorhandler dies	
		{'EXIT', _Pid, W} ->
			AfterLastSentIdVector = recoverClientSupervisor(State, W), 
			AfterSubscription = Subscription,
			Msg = supervisor_crash;
		
		%% TODO sent empty messaged with MsgId, to unblock the queue
		{unicast_not_completed, ReceiverInfo, _MsgId, Message}-> 
			printVerboseLevel("~n~s ---- Unicast NOT completed for sender: ~w, receiver:~w, message: ~w~n", 
					  [node(),{client, node()}, ReceiverInfo, Message], 1), 
			Msg = [], AfterSubscription = Subscription, AfterLastSentIdVector = LastSentIdVector;
		
		{unicast_completed, ReceiverInfo, _MsgId, Message} -> 
			printVerboseLevel("~n~s ---- Unicast completed for sender: ~w, receiver:~w, message: ~w~n", 
					  [node(),{client, node()}, ReceiverInfo, Message], 5), 
			Msg = [], AfterSubscription = Subscription, AfterLastSentIdVector = LastSentIdVector;
		
		{multicast_completed,  ReceiversList, Message, _OldLastSentIdVector}->
			printVerboseLevel("~n~s ---- Multicast completed for sender: ~w, receiverslist:~w, message: ~w~n", 
					  [node(),{client, node()}, ReceiversList, Message], 5), 
			Msg = [], AfterSubscription = Subscription, AfterLastSentIdVector = LastSentIdVector;
		
		{multicast_not_completed, ReceiversList, Message, _OldLastSentIdVector}->
			printVerboseLevel("~n~s ---- Multicast NOT completed for sender: ~w, receiverslist:~w, message: ~w~n", 
					  [node(),{client, node()}, ReceiversList, Message], 1),
			Msg = [], AfterSubscription = Subscription, AfterLastSentIdVector = LastSentIdVector;

		
		Msg-> 
			printVerboseLevel("~nCLIENT@~s> *** Received message: ~nCLIENT@~s> *** ~w~n", 
							  [node(), node(), Msg], 4 + ?verbal_handicap),
			AfterSubscription = Subscription, AfterLastSentIdVector = LastSentIdVector
	after ?inactivity_timeout->
		case Subscription of
			{BrokerInfo, _}->
				case utility:remoteIsRegNameAlive(BrokerInfo) of
				true->
					AfterSubscription = Subscription;
				false->	
					printVerboseLevel("~nCLIENT@~s> Broker with subscription is not alive:~w, continuing to live~n", 
			 		[node(), BrokerInfo],1 + ?verbal_handicap),
					AfterSubscription = []
				end;
			_ -> AfterSubscription = Subscription
		end, Msg = inactivity, AfterLastSentIdVector = LastSentIdVector
	end,
	
	case Msg of
		[]-> AReceivingQueue =  ReceivingQueue;
		supervisor_crash -> AReceivingQueue =  ReceivingQueue;
		inactivity ->  AReceivingQueue =  ReceivingQueue;
		_-> AReceivingQueue = ReceivingQueue ++ [Msg]
	end,
	
	{NewReceivingQueue, NewLastDeliveredIdVector,ReadyMsgs} = 
		returnMsgsToBeElaborated(client,AReceivingQueue, LastDeliveredIdVector),
	printVerboseLevel("~nCLIENT@~s> Found ready msgs ~w, remaining msgs ~w~n", 
			 [node(), ReadyMsgs, NewReceivingQueue],5 + ?verbal_handicap),
	
	%% Elaborate all ready msgs in order
	{NewSubscription, NewLastSentIdVector} = clientElaborateMsgs(AfterSubscription, AfterLastSentIdVector, ReadyMsgs, CallbackFunction),
	printVerboseLevel("~nCLIENT@~s> NewLastSentIdVector:~w, NewSubscription: ~w~n", 
			 [node(), NewLastSentIdVector, NewSubscription],5 + ?verbal_handicap),
	
	case Msg of
		[]-> 
			NewState = [NewSubscription, NewReceivingQueue,  NewLastDeliveredIdVector, NewLastSentIdVector, CallbackFunction];	
		_->
			%% Send state to supervisor
			NewerLastSentIdVector = incrementLastSentId({supervisor_client, node()}, NewLastSentIdVector),
			NewState = [NewSubscription, NewReceivingQueue,  NewLastDeliveredIdVector, NewerLastSentIdVector, CallbackFunction],	
			sendStateToSupervisor(client, NewState, NewLastSentIdVector)
	end,
	clientLoop(NewState)
catch
	Why->
	   printVerboseLevel("~nCLIENT@~s> EXCEPTION: ~w~nCLIENT@~s> Client restarted~n ",
			[node(),Why,node()],1 + ?verbal_handicap),
	   clientLoop(State)
end.

%%%
%%% Client Elaborate Msgs
%%%

clientElaborateMsgs(Subscription, LastSentIdVector, [], _CallbackFunction) ->
    {Subscription, LastSentIdVector};

clientElaborateMsgs(Subscription, LastSentIdVector, ReadyMsgs, CallbackFunction)->	
 	[FirstMsg| Rest] = ReadyMsgs,
    {_AckReplyInfo, _SenderInfo, _MsgId, Content} = FirstMsg,
	
	 case Content of
		{publication_forward, _BrokerInfo, Message} -> 
	    	printVerboseLevel("~nCLIENT@~s> *** Received ***: '~s' ~n",[node(),Message],1),
			{NewSubscription, NewLastSentIdVector} =
				applyCallbackFunction(Subscription, LastSentIdVector, Message, CallbackFunction),	
	    	clientElaborateMsgs(NewSubscription, NewLastSentIdVector, Rest, CallbackFunction);
		 
		{publish_req, BrokerInfo, Message}->
			{NewSubscription, NewLastSentIdVector} = 
				publicationForwarding(Subscription,node(), BrokerInfo, 
													   Message, LastSentIdVector),
			clientElaborateMsgs(NewSubscription, NewLastSentIdVector, Rest, CallbackFunction);
		   
		{subscribe_req, BrokerInfo, MatchingFunction} ->	
			{NewSubscription, NewLastSentIdVector} = 
				subscriptionForwarding(Subscription, node(), BrokerInfo, MatchingFunction, 
										 LastSentIdVector),
			clientElaborateMsgs(NewSubscription, NewLastSentIdVector, Rest, CallbackFunction);

		{unsubscribe_req, BrokerInfo, MatchingFunction}->
			{NewSubscription, NewLastSentIdVector} = 
				unsubscriptionForwarding(Subscription, node(), BrokerInfo, MatchingFunction, 
										 LastSentIdVector),
			clientElaborateMsgs(NewSubscription, NewLastSentIdVector, Rest, CallbackFunction);
			
		Any -> 
			printVerboseLevel("CLIENT@~s> Unexpected message received ~w ~n", [node(),Any],1 ),
			{Subscription, LastSentIdVector}
	end.


unsubscriptionForwarding(Subscription, ClientNode, BrokerInfo, MatchingFunction, LastSentIdVector) ->
	if Subscription =:= {BrokerInfo, MatchingFunction} -> 
		%% If broker is alive
		case utility:remoteIsRegNameAlive(BrokerInfo) of
		true->
		   printVerboseLevel("~nCLIENT@~s> Sent UNSUBSCRIBE to broker @~w with ~w~n",
			  [node(),BrokerInfo, MatchingFunction],2),
			NewLastSentIdVector = unicastSend({client, ClientNode}, 
			{unsubscribe, ClientNode, MatchingFunction}, BrokerInfo,  LastSentIdVector);
		false-> printVerboseLevel("~nCLIENT@~s> ~w is not alive, didn't send unsubscription~n",
			  [node(),BrokerInfo],1 ), NewLastSentIdVector = LastSentIdVector
		end,
		NewSubscription = [];
	true-> NewSubscription = Subscription, NewLastSentIdVector = LastSentIdVector,
		   printVerboseLevel("~nCLIENT@~s> Error: Tried to send UNSUBSCRIBE to broker @~w for not subscribed function: ~w~n",
			 [node(),BrokerInfo, MatchingFunction],2)
	end,
	{NewSubscription,NewLastSentIdVector}.

subscriptionForwarding([], ClientNode, BrokerInfo, MatchingFunction, LastSentIdVector) ->
%% Is Broker alive?
case utility:remoteIsRegNameAlive(BrokerInfo) of
true->
	printVerboseLevel("~nCLIENT@~s> Sent SUBSCRIBE to broker @~w with ~w~n",
			  [node(),BrokerInfo, MatchingFunction],2),
	NewLastSentIdVector = unicastSend({client, ClientNode}, 
			{subscribe, ClientNode, MatchingFunction}, BrokerInfo,  LastSentIdVector),
		{{BrokerInfo, MatchingFunction},NewLastSentIdVector};
false->
	printVerboseLevel("~nCLIENT@~s> ~w is not alive, didn't send subscription~n",
		[node(),BrokerInfo],1), {{[],[]},LastSentIdVector}
end;
	
subscriptionForwarding(_, ClientNode, BrokerInfo, MatchingFunction, LastSentIdVector) ->
%% Is Broker alive?
case utility:remoteIsRegNameAlive(BrokerInfo) of
true->
	NewLastSentIdVector= unicastSend({client, ClientNode}, 
		{unsubscribe, ClientNode, MatchingFunction}, BrokerInfo,  LastSentIdVector),
	subscriptionForwarding([], ClientNode, BrokerInfo, 
			  MatchingFunction, NewLastSentIdVector);
false->
	printVerboseLevel("~nCLIENT@~s> ~w is not alive, didn't send subscription~n",
		[node(),BrokerInfo],1), {{[],[]},LastSentIdVector}
end.

publicationForwarding(Subscription, ClientNode, BrokerInfo, Message, LastSentIdVector)->
	%% Is Broker alive?
	case utility:remoteIsRegNameAlive(BrokerInfo) of
		true->
			printVerboseLevel("~nCLIENT@~s> Sent PUBLISH to broker @~w with ~s~n",
			  [node(),BrokerInfo, Message],2 ),
			NewLastSentIdVector = 
				unicastSend({client, ClientNode}, {publish, ClientNode, Message}, BrokerInfo,  LastSentIdVector),
			{Subscription, NewLastSentIdVector};
		false-> 
			printVerboseLevel("~nCLIENT@~s> ~w is not alive, didn't send publish~n",
			  [node(),BrokerInfo],1 ), {[], LastSentIdVector}
		end.


%%%
%%% Making the calls appear in the right order for each client node (hopefully :)
%%% initnext/1 gets called in start/1
%%%

getNextId(Node)->
	NextName = list_to_atom("next_" ++  atom_to_list(Node)),
	NextName!{next, self()},
	receive
		{Value}-> Value
	after 500-> 1
	end.
	
initnext(Node)->
	Pid = spawn(fun() -> next(1) end),
	NextName = list_to_atom("next_" ++  atom_to_list(Node)),
	utility:registerName(NextName, Pid).

next(Value)->
	printVerboseLevel("~n *** Next value: ~w~n",[Value],7 + ?verbal_handicap), 
	receive
		{next, Pid}->	Pid!{Value},
						next(Value+1)
	end.

%%%
%%% Applying callback function Mod:Fun to Message
%%% Callback Function requirements: has inputs Msg and Node and returns {Value, NewMessage}
%%% If Value == send, then the NewMessage is published
%%%

applyCallbackFunction(Subscription, LastSentIdVector, Message, CallbackFunction) ->
try
	case CallbackFunction of
		[]-> {Subscription, LastSentIdVector};
		Any ->
			case Any of	
				{Mod,Fun} -> {Value, NewMessage} = Mod:Fun(Message, node());
				 _ -> {Value, NewMessage} = CallbackFunction(Message, node())
			end,
			printVerboseLevel("~n *** Callback Return Value: ~w Message: ~s ~n",[Value, NewMessage],8 + ?verbal_handicap),
			case Value of 
				send-> 
					case Subscription of   
						{BrokerInfo, _}-> ok;
						[]-> BrokerInfo = {dispatcher, dispatcher:dispatcher_node()}
					end,
					publicationForwarding(Subscription, node(), BrokerInfo,NewMessage, LastSentIdVector);
				_-> {Subscription, LastSentIdVector}
			end
	end
catch
	_ -> printVerboseLevel("~n Bad Callback Function, ignoring it ~n",[],1 + ?verbal_handicap),
		{Subscription, LastSentIdVector}
end.


%% printClientState(Node, State)->
%% 	[Subscription, ReceivingQueue, LastDeliveredIdVector, LastSentIdVector, CallbackFunction] = State,
%% 	Line = "~n------------------------------------------------------------~n",
%% 	printVerboseLevel(Line ++ 
%% 		"CLIENT~s> Subscription: ~w, ReceivingQueue: ~w, LastDeliveredIdVector: ~w, LastSentIdVector:~w, CallbackFunction: ~w~n" ++ Line, 
%% 		[Node, Subscription, ReceivingQueue, LastDeliveredIdVector, LastSentIdVector, CallbackFunction],4).
%% 	