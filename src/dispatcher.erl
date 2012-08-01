%% Author: Sara Magliacane
%% Created: 29/ago/2010
%% Description: Wrapper module for the "dispatcher" kind of broker
-module(dispatcher).

%%
%% Include files
%%
-import(utility,[registerName/2,  printVerboseLevel/3]).
-import(datautil, [computeDestinations/2, addInterest/3, deleteInterest/3, deleteClient/2,
	 extractAllMatchingFunctions/1, extractMatchingFunctions/2, printInterests/1]).
-import(errorhandler, [on_exit/5]).
%%
%% Exported Functions
%%
-export([start/0, init/1, dispatcher_node/0, printState/0]).


%%
%% API Functions
%%

dispatcher_node() -> 
	sara@sara.

start()-> 
	Pid = spawn(dispatcher_node(),fun()-> init([]) end),
    on_exit(Pid, dispatcher_node(), dispatcher, dispatcher, init).

printState()-> 
	{dispatcher, dispatcher_node()}!{{noreply}, {dummy}, 1,{printstate}}.


%%
%% Local Functions
%%

init(State) ->
	printVerboseLevel("~nDISPATCHER@~s> Dispatcher started~n", [node()], 3),
	
	registerName(dispatcher, self()),
	%% in order to recover the error handler 
	process_flag(trap_exit, true),
	
	if State ==	[]-> NewState = {[],[],[],[],[],[]};
	   true->{_Type, NewState, {_FatherInfo, _OldPendingFatherInfo}} = State
	end,
	broker:brokerLoop(dispatcher, NewState, {no_father, no_father} ).

