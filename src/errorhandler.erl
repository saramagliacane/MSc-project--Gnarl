%% Author: Sara Magliacane
%% Created: Aug 30, 2010
%% Description: TODO: Add description to errorhandler
-module(errorhandler).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([on_exit/5, sendStateToSupervisor/3, onSenderExit/8, recoverBrokerSupervisor/4, recoverClientSupervisor/2]).
-import(utility, [remoteIsProcessAlive/1, remotewhereis/2, registerName/2, removeFromLastIdVector/2, incrementLastSentId/2,
	 remoteIsRegNameAlive/2, unicastSend/4, multicastSend/4,  
	 returnMsgsToBeElaborated/3,printVerboseLevel/3]).

%%
%% API Functions
%%

%%%
%%% The on_exit function is the basic local fault tolerance mechanism, which spawns the errorHandlerLoop 
%%% on the same node as the process to be supervised and registers its name as supervisor_<process_name>
%%%

on_exit(Pid, Node, RegName, Mod, Fun)->
   spawn(Node, fun()->
		 process_flag(trap_exit, true),
		 link(Pid),
		 printVerboseLevel("~n~s@~s> Error handler started ~n" , [string:to_upper(atom_to_list(RegName)), Node],4),
		 SupervisorName = list_to_atom(string:concat("supervisor_", atom_to_list(RegName))),
		 utility:registerName(SupervisorName, self()),
		 errorHandlerLoop(RegName, SupervisorName, Mod, Fun)
		 end).

%%%
%%% The onSenderExit function spawns a special error handler for the sender processes (sendAndWaitForAck).
%%% It spawns an error handler which doesn't need state updates, since the state is the message
%%% to be sent. Sender processes are very short, so they could have already finished before the handler
%%% manages to link to them. In this case the handler respawns the sender process, because it is not
%%% sure whether the process crash or completed. Anyway, the receiver will ack it and discard it if it's old
%%%

onSenderExit(Pid, Node, SendType, _MulticastBoss, SenderInfo, Message, ReceiverInfo, LastSentIdVector)->
spawn(Node, fun()->
		process_flag(trap_exit, true),
		%% if the process is alive, link to it; if it's not, respawn it anyway
		case is_process_alive(Pid) of
		true-> 
			link(Pid),
			printVerboseLevel("~n~w> Error handler for sender process started ~n" , [Node],4);
		false->
			printVerboseLevel("~n~w> Could not link to Pid, in doubt, respawning ~n", [ Node],1),
			case SendType of
				unicast->
					spawn(fun()-> utility:unicastSendLoop(SenderInfo, Message, ReceiverInfo,  LastSentIdVector) end);
				multicast->
					spawn (fun()-> utility:multicastSendLoop(SenderInfo, Message, ReceiverInfo, LastSentIdVector, [], ReceiverInfo, LastSentIdVector) end)
			end
		end,
		receive
		     {'EXIT', Pid, normal} -> 
				 printVerboseLevel("~n~w> Sender process exited normally~n",
					   [node()],5);
			 {'EXIT', Pid, Why}->
				 printVerboseLevel("~n~w> Sender Process crashed. Why? ~w, , respawning with SenderInfo: ~w, Message: ~w, ReceiverInfo:~w, LastSentIdVector: ~w~n",
						[ node(), Why, SenderInfo, Message, ReceiverInfo, LastSentIdVector],1),
				case SendType of
					unicast->
						spawn(fun()-> utility:unicastSendLoop(SenderInfo, Message, ReceiverInfo,  LastSentIdVector) end);
					multicast->
						spawn (fun()-> utility:multicastSendLoop(SenderInfo, Message, ReceiverInfo, LastSentIdVector, [], ReceiverInfo, LastSentIdVector) end)
				end
		end		  
		
	end).

%%%
%%% The sendStateToSupervisor function sends the state update of the process
%%% to the supervisor using the unicastSend primitive in order to assure it is delivered in order.
%%% If the supervisor is dead, it prints an error message. (could respawn it, Mod is RegName and Fun is restart/2)
%%% As other functions using unicastSend, it returns an updated version of the LastSentIdVector
%%%

sendStateToSupervisor(RegName, State, LastSentIdVector) ->
	%% name of the supervisor
	Supervisor = list_to_atom(string:concat("supervisor_", atom_to_list(RegName))),
	case whereis(Supervisor) of 
		%% the process doesn't exist (or it is not registered yet)
		undefined -> 
			printVerboseLevel("~n~s@~s> ERROR: tried to sent state to a non-existing supervisor ~n" , [string:to_upper(atom_to_list(RegName)), node()],1),
			LastSentIdVector;
		%% the process exists and is succesfully registered
		_Pid -> 
			printVerboseLevel("~n~s@~s> Sent state to supervisor ~n" , [string:to_upper(atom_to_list(RegName)), node()],4),
			unicastSend({RegName, node()}, {record_state, State}, {Supervisor, node()}, 
							LastSentIdVector)
			
	end.


recoverBrokerSupervisor(Type, State, FatherInfo, Why)->
	on_exit(self(), node(), Type, Type, init), 
	SupervisorName = list_to_atom(string:concat("supervisor_", atom_to_list(Type))),
	{ListOfClients, ListOfBrokers, ReceivingQueue, LastDeliveredIdVector, LastSentIdVector, StillReadyMsgs} = State,
	
	%% or it will not deliver the state update
	NewLastSentIdVector = removeFromLastIdVector([{SupervisorName, node()}], LastSentIdVector),
	
	NewerLastSentIdVector = incrementLastSentId([{SupervisorName, node()}], NewLastSentIdVector),
	NewState = {ListOfClients, ListOfBrokers, ReceivingQueue, LastDeliveredIdVector, NewerLastSentIdVector, StillReadyMsgs},
	printVerboseLevel("~n~s@~s> Error handler died: ~w, respawning and updating its state~n", [string:to_upper(atom_to_list(Type)), node(), Why], 1 ),
	sendStateToSupervisor(Type, {Type, NewState, FatherInfo}, NewerLastSentIdVector).
	

recoverClientSupervisor(State, Why) ->
	on_exit(self(), node(), client, client, init),
	printVerboseLevel("~nCLIENT@~s> Error handler died: ~w, respawning ~n", [node(), Why],1),
	
	[Subscription, ReceivingQueue, LastDeliveredIdVector, LastSentIdVector, CallbackFunction] = State,
	
	%% or it will not deliver the state update
	NewLastSentIdVector = removeFromLastIdVector([{supervisor_client, node()}], LastSentIdVector),
	
	%% Send state to supervisor
	NewerLastSentIdVector = incrementLastSentId([{supervisor_client, node()}], NewLastSentIdVector),
	NewState = [Subscription, ReceivingQueue,  LastDeliveredIdVector, NewerLastSentIdVector, CallbackFunction],	
	
	sendStateToSupervisor(client, NewState, NewLastSentIdVector).	
									

%%%
%%% The errorHandlerLoop is the main loop which receives the state of the supervised process 
%%% and in case it dies, applies the function which was passed as arguments at its creation
%%% The state of the errorHandlerLoop includes a ReceivingQueue and a LastDeliveredVector;
%%% these variables are useful to deliver and apply state updates in order, which is done 
%%% respectively by the functions returnMsgsToBeElaborated and errorHandlerElaborate
%%%

errorHandlerLoop(RegName, SupervisorName, Mod, Fun) ->
	errorHandlerLoop(RegName, SupervisorName, [], [], [],Mod, Fun).
		 
errorHandlerLoop(RegName, SupervisorName, State, ReceivingQueue, LastDeliveredIdVector, Mod, Fun)->
receive
		 
		%% mission accomplished, the process exited normally
		{'EXIT', Pid, normal} -> 
			printVerboseLevel("~n~w> Process ~w on node ~w exited normally ~n",
				   [node(), Pid, node()],1),
			unregister(SupervisorName), Msg = exit, NewLastDeliveredIdVector = LastDeliveredIdVector;
		{'EXIT', Pid, [{_,_,normal},_,_]} ->
			printVerboseLevel("~n~w> Process ~w on node ~w exited normally ~n",
				   [node(), Pid, node()],1),
			unregister(SupervisorName), Msg = exit, NewLastDeliveredIdVector = LastDeliveredIdVector;		
		%% the process crashed, applying chosen function with most recent state
		{'EXIT', Pid, Why}->
			printVerboseLevel("~n~w> Process ~w on node ~w crashed. Why? ~w~n~w> Applying ~w:~w with State: ~w ~n",
				   [node(), Pid, node(), Why, node(), Mod, Fun,State], 1),
			%% apply function and reset LastDeliveredIdVector
			spawn_link(fun()-> apply(Mod,Fun,[State]) end), 
			%%NewLastDeliveredIdVector = removeFromLastIdVector([{RegName, node()}], LastDeliveredIdVector),
			NewLastDeliveredIdVector = LastDeliveredIdVector,
			Msg = respawned;
		
		%% receiving other messages, hopefully state updates
		Msg -> NewLastDeliveredIdVector = LastDeliveredIdVector				
end,
case Msg of 
	%% state updates are received and put in ReadyMsgs if they can be delivered, 
	%% otherwise they stay in the queue
	{_,_,_,{record_state,_}}->
		{NewReceivingQueue, NewerLastDeliveredIdVector,ReadyMsgs} = 
			returnMsgsToBeElaborated(SupervisorName,ReceivingQueue ++ [Msg], NewLastDeliveredIdVector),
		printVerboseLevel("~s@~s> Found ready msgs ~w, remaining msgs ~w~n", 
			 [string:to_upper(atom_to_list(SupervisorName)), node(), ReadyMsgs, NewReceivingQueue],5),
		NewState = errorHandlerElaborate(ReadyMsgs, State),
		printVerboseLevel("~n~s@~w received State ~w ~n" , 
					  [string:to_upper(atom_to_list(SupervisorName)), node(),NewState],4),
		errorHandlerLoop(RegName, SupervisorName, NewState, NewReceivingQueue, NewerLastDeliveredIdVector, Mod, Fun);
	%% unexpected messages handling
	respawned -> errorHandlerLoop(RegName, SupervisorName, State, ReceivingQueue, NewLastDeliveredIdVector, Mod, Fun);
	exit -> ignore;
	Any -> printVerboseLevel("~n~s@~s> Unexpected message: ~w~n" , [string:to_upper(atom_to_list(SupervisorName)), node(), Any],1)
end.

%%%
%%% Utility function elaborating ready messages for the error handler 
%%% by taking the last legal State and applying it
%%%

errorHandlerElaborate([],State)->
	State;
errorHandlerElaborate(ReadyMsgs,_)->
	Msg = lists:last(ReadyMsgs),
	{_,_,_,{record_state, NewState}} = Msg,
	NewState.
	
	
