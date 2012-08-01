%% Author: Sara Magliacane
%% Created: Sep 4, 2010
%% Description: TODO: Add description to datautil
-module(datautil).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([computeDestinations/2, matches/3, addInterest/3, 
		 addMoreInterests/3, deleteInterest/3, deleteInterestAndRemoveEmpty/3,
		 deleteClient/2, deleteClients/2, deleteMultipleInterest/3,
	 extractAllMatchingFunctions/1, extractMatchingFunctions/2, 
		 printInterests/3, returnDeadProcessesInList/1]).
-import(utility, [ printVerboseLevel/3]).
%%
%% API Functions
%%

%%% ******************* List Managment ****************************

%%% Compute destinations of the published message
computeDestinations(Message, Interests) ->
    computeDestinations(Message, Interests, []).
computeDestinations(_, [], Result) -> Result;
computeDestinations(Message, [{Client, ListOfMatchingFunctions}|T], Result) ->
    Matches = matches(Message, Client, ListOfMatchingFunctions),
    if Matches == true ->
	    computeDestinations(Message, T, Result ++ [{Client, ListOfMatchingFunctions}]);
	Matches == false ->
	    computeDestinations(Message, T, Result)
    end.

%%% Is there any function which matches the message?
matches(_,_,[]) -> false;
matches(Message,Client, [FirstMatchingFunction|OtherMatchingFunctions]) -> 
case FirstMatchingFunction of 
	[]-> matches(Message,Client,OtherMatchingFunctions);
	_Else->
    try FirstMatchingFunction(Message) of
		true -> printVerboseLevel("NODE@~s> Message '~s' matches function ~w~n",
						  [node(),Message, FirstMatchingFunction],5),
			true;				      
		false -> matches(Message,Client,OtherMatchingFunctions) 
    catch
	_: Why -> 
		io:format("NODE@~s> Matching function exception ~w : ~w ~n",[node(), FirstMatchingFunction, Why]),
		 matches(Message,Client,OtherMatchingFunctions)
     end
end.

%%% Add Item to Interest list - adding an existing function doesn't produce side effects
addInterest(Interests, Client, MatchingFunction) ->
    addInterest(Interests, Client, MatchingFunction, []).
addInterest([], Client, [], Result) ->
	Result ++ [{Client, []}];
addInterest([], Client, MatchingFunction, Result) ->
    Result ++ [{Client, [MatchingFunction]}];
addInterest([{SelectedClient, Interests}|T], Client, MatchingFunction, Result) ->
    if SelectedClient == Client ->
			if Interests =:= [] -> 
				   if MatchingFunction =:= [] -> NewInterests = [];
					 true->	NewInterests = [MatchingFunction]
				   end;
		   		true-> NewInterests = Interests ++ [MatchingFunction]
			end,
	    	Result ++ [{Client, NewInterests}] ++ T;
       SelectedClient =/= Client ->
	    	addInterest(T, Client, MatchingFunction, Result ++ [{SelectedClient, Interests}])
    end.

%%% Add multiple matching functions to client
addMoreInterests(Interests, Client, [])->
	NewInterests = addInterest(Interests, Client, []),
	printVerboseLevel(">>> addMoreInterests: ~w ",[NewInterests],8),
	NewInterests;

addMoreInterests(Interests, Client, [H|T])->
	NewInterests = addInterest(Interests, Client, H),
	addMoreInterests(NewInterests, Client, T).

%%% Delete Item from Interest list - deleting an non existing function doesn't produce side effects
deleteInterest(Interests, Client, MatchingFunction) ->
    deleteInterest(Interests, Client, MatchingFunction, []).
deleteInterest([],_, _, Result) ->
    Result;
deleteInterest([{SelectedClient, Interests}|T], Client, MatchingFunction, Result) ->
    if SelectedClient == Client ->
	    NewInterests = Interests -- [MatchingFunction],
	    Result ++ [{Client, NewInterests}] ++ T;
       SelectedClient =/= Client ->
	    deleteInterest(T, Client, MatchingFunction, Result ++ [{SelectedClient, Interests}])
    end.

%%% Delete Item from Interests Lists, in case result is empty, remove it
deleteInterestAndRemoveEmpty(Interests, Client, MatchingFunction)->
	deleteInterestAndRemoveEmpty(Interests, Client, MatchingFunction, []).
deleteInterestAndRemoveEmpty([], _, _, Result)->
	Result;
deleteInterestAndRemoveEmpty([{SelectedClient, Interests}|T], Client, MatchingFunction, Result)->
	 if SelectedClient == Client ->
	    NewInterests = Interests -- [MatchingFunction],
		case string:len(NewInterests) of
			0 -> Result ++ T;
	    	_ -> Result ++ [{Client, NewInterests}] ++ T
		end;
      SelectedClient =/= Client ->
	    deleteInterestAndRemoveEmpty(T, Client, MatchingFunction, Result ++ [{SelectedClient, Interests}])
    end.

%%% Deletes a list of matching functions
deleteMultipleInterest(Interests, _Client, [])->
	Interests;
deleteMultipleInterest(Interests, Client, [H|T])->
	NewInterests = deleteInterest(Interests, Client, H), 
	deleteMultipleInterest(NewInterests, Client, T).

%%% Deletes completely a client, if it exists
deleteClient(Interests, []) ->
	Interests;
deleteClient(Interests, Client) ->
    deleteClient(Interests, Client, []).
deleteClient([],_, Result)->
    Result;
deleteClient([{SelectedClient, Interests}|T], Client,  Result) ->
    if SelectedClient == Client ->
	    Result ++ T;
       SelectedClient =/= Client ->
	    deleteClient(T, Client,  Result ++ [{SelectedClient, Interests}])
    end.

%%% Deletes more clients
deleteClients(Interests, [])->
	Interests;	
deleteClients(Interests, [H|RestOfListOfClients])->
	NewInterests = deleteClient(Interests, H),
	deleteClients(NewInterests, RestOfListOfClients).


%%% returns a list of all matching functions from a ListOfBrokers or ListOfClients 
extractAllMatchingFunctions(ListOfClients) ->
    extractAllMatchingFunctions(ListOfClients, []).
extractAllMatchingFunctions([], Result)-> Result;
extractAllMatchingFunctions([{_SelectedClient, Interests}|T], Result)->
	if Interests =:= [] ->
		   extractAllMatchingFunctions(T, Result);
	   true -> extractAllMatchingFunctions(T, Result ++ Interests)
	end.
	

extractMatchingFunctions(ListOfBrokers, BrokerInfo) ->
	
printVerboseLevel("~n>>> extractMatchingFunctions ListOfBrokers:~w BrokerInfo: ~w~n",[ListOfBrokers, BrokerInfo],8),	
try
	case lists:keyfind(BrokerInfo,1,ListOfBrokers) of
		false -> [];
		{BrokerInfo, ListOfMatchingFunctions} -> ListOfMatchingFunctions
	end
catch
	Any-> printVerboseLevel("~n>>> extractMatchingFunctions ~w ~n Exception: ~w ~n",[ListOfBrokers, Any],1)
end.
	

printInterests(Type, ListOfClients, ListOfBrokers)->
   printVerboseLevel("~n~s@~w> ListOfClients: ~w, ListOfBrokers:~w ~n",[Type, node(), ListOfClients, ListOfBrokers], 1).

%%% Nomen est omen :)
returnDeadProcessesInList(ListOfClients)->
	returnDeadProcessesInList(ListOfClients, []).
returnDeadProcessesInList([], Result)->
	Result;
returnDeadProcessesInList([{ClientInfo, _Interests}|T], Result)->
	case utility:remoteIsRegNameAlive(ClientInfo) of
		true -> returnDeadProcessesInList(T, Result);
		false -> returnDeadProcessesInList(T, Result ++ [ClientInfo])
	end.



