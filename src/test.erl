%% Author: Sara Magliacane
%% Created: Aug 30, 2010
%% Description: TODO: Add description to test
-module(test).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([test/0, treetest/0, testCallbackFunction/0, testCallbackFunction/1, treetestD/0,
		  testCallbackFunctionD/0, testCallbackFunctionD/1, testD/0,
		 msgcontains/2, shorterthan/2, nonzerolenght/1, lenghtbetween/3]).

%%
%% API Functions
%%

test()->
    io:format("\n \n *** BASIC TEST ROUTINE *** \n\n"),
	io:format("0) There are two brokers directly sons of dispatcher@sara@sara. \n"),
	io:format("1) The client@pippo subscribes with 'lenghtbetween(Msg,1,5)' to broker1@pippo. \n"),
	io:format("2) The client2@pippo subscribes with 'lenghtbetween(Msg,6,20)' to broker2@pippo. \n"),
	io:format("3) The client@pippo publishes a 'Message to Broker1' to broker1@pippo \n"),
	io:format("4) The client2@pippo publishes a 'Ciao' to broker2@pippo \n"),
	io:format("5) The client@pippo unsubscribes from broker1@pippo\n"),
	io:format("6) The client3@pippo publishes a 'NO!' to broker1@pippo \n"),
	dispatcher:start(),
    broker:start('broker1@pippo'),
    broker:start('broker2@pippo'),
    Function1 = fun(Msg)->lenghtbetween(Msg,1,5) end,
	Function2 = fun(Msg)->lenghtbetween(Msg,6,20) end,
    client:subscribe('client@pippo','broker1@pippo', Function1 ),
	client:subscribe('client2@pippo','broker2@pippo', Function2 ),
	
	timer:sleep(500),
	io:format("\n\n\n ---------------State of the brokers:---------------\n"),
	dispatcher:printState(),
	broker:printState(broker1@pippo),
	broker:printState(broker2@pippo),
	
	timer:sleep(500),
    client:publish('client@pippo','broker1@pippo',"Message to Broker1"),	
    client:publish('client2@pippo','broker1@pippo',"Ciao"),
	client:unsubscribe('client@pippo','broker1@pippo',Function1),
    client:publish('client3@pippo','broker1@pippo',"NO!"),
	ok.

treetest()->
	io:format("\n \n *** TREETEST ROUTINE *** \n\n"),
	io:format("1) We start 3 clients on client@pippo, client2@pippo, client3@pippo\n"),
	io:format("2) We start a broker on broker1@pippo, directly connected to the dispatcher\n"),
	io:format("3) We start a broker on broker2@pippo, connected to broker on broker1@pippo\n"),
	io:format("4) client@pippo subscribes on broker2@pippo with function nonzerolenght\n"),
	io:format("5) client2@pippo subscribes on broker1@pippo with function nonzerolenght\n"),
	io:format("6) client3@pippo subscribes on broker1@pippo with function nonzerolenght\n"),
	io:format("7) client@pippo publishes 'Messaggio di provaaa' on broker2@pippo\n"),
	io:format("8) client2@pippo publishes 'Ciaooo' on broker1@pippo\n"),
	
	dispatcher:start(),
	broker:start('broker1@pippo'),
	client:start('client@pippo'),
	client:start('client2@pippo'),
	client:start('client3@pippo'),
	broker:start('broker2@pippo', 'broker1@pippo'),
	
	F = fun(Msg) -> nonzerolenght(Msg) end,
	G = fun(Msg) -> nonzerolenght(Msg) end,
	H = fun(Msg) -> nonzerolenght(Msg) end,
	
	client:subscribe('client@pippo', 'broker2@pippo', F ),
	client:subscribe('client2@pippo', 'broker1@pippo', G ),
	client:subscribe('client3@pippo', 'broker1@pippo', H ),
	
	timer:sleep(500),
	io:format("\n\n\n ---------------State of the brokers:---------------\n"),
	dispatcher:printState(),
	broker:printState(broker1@pippo),
	broker:printState(broker2@pippo),
	
	timer:sleep(500),
	client:publish('client@pippo','broker2@pippo', "Messaggio di provaaa"),
	client:publish('client2@pippo','broker1@pippo', "Ciaooo"),
		
	ok.

testCallbackFunction()->
	testCallbackFunction(2).

testCallbackFunction(NumberOfIterations)->
	io:format("\n \n *** CALLBACK TEST ROUTINE *** \n\n"),
	io:format("0) There are two brokers directly sons of dispatcher@sara@sara. \n"),
	io:format("1) Client@pippo and client2@pippo subscribe with nonzerolenght to broker1@pippo and Callback function which adds ReplyTo to each recevied message which is not itself a reply. \n"),
	io:format("2) EXTERN@pippo subscribes with the nonzerolength matching function to broker2@pippo, no Callback function\n"),
	io:format("3) Client@pippo publishes a 'Hey!' to broker1@pippo \n"),
	io:format("4) EXTERN@pippo sees the messages in order.\n"),
	dispatcher:start(),
	broker:start('broker1@pippo'),
    broker:start('broker2@pippo'),
	
	NewString = formateString(NumberOfIterations, "Re: '"),
	
	CallbackFunction = fun (Msg, Node) -> 
								Value = msgcontains(NewString,Msg), 
								case Value of
									true ->{none, none}; 
									false ->{send, "Re: '" ++ Msg ++ "' from :" ++ string:to_upper(atom_to_list(Node))} 
								end 
					   end,
								
	client:start('client@pippo', CallbackFunction),
	client:start('client2@pippo', CallbackFunction),
	client:start('EXTERN@pippo'),
	
	F = fun(Msg) -> nonzerolenght(Msg) end,
	G = fun(Msg) -> nonzerolenght(Msg) end,
	H = fun(Msg) -> nonzerolenght(Msg) end,
	client:subscribe('client@pippo','broker1@pippo', F ),
	client:subscribe('client2@pippo','broker1@pippo', G ),
	client:subscribe('EXTERN@pippo','broker2@pippo', H ),
    
	timer:sleep(500),
	io:format("\n\n\n ---------------State of the brokers:---------------\n"),
	dispatcher:printState(),
	broker:printState(broker1@pippo),
	broker:printState(broker2@pippo),
	
	timer:sleep(500),
	io:format("\n\n\n ----------------Publish----------------------------\n"),
	client:publish('client@pippo','broker1@pippo',"Hey!"),	
   	ok.

testD()->
    io:format("\n \n *** BASIC TEST ROUTINE *** \n\n"),
	io:format("0) There are two brokers directly sons of dispatcher@sara@sara. \n"),
	io:format("1) The client@pippo subscribes with 'lenghtbetween(Msg,1,5)' to broker1@davide. \n"),
	io:format("2) The client2@davide subscribes with 'lenghtbetween(Msg,6,20)' to broker2@pippo. \n"),
	io:format("3) The client@pippo publishes a 'Message to Broker1' to broker1@davide \n"),
	io:format("4) The client2@davide publishes a 'Ciao' to broker2@pippo \n"),
	io:format("5) The client@pippo unsubscribes from broker1@davide\n"),
	io:format("6) The client3@pippo publishes a 'NO!' to broker1@davide \n"),
	dispatcher:start(),
    broker:start('broker1@davide'),
    broker:start('broker2@pippo'),
    Function1 = fun(Msg)->lenghtbetween(Msg,1,5) end,
	Function2 = fun(Msg)->lenghtbetween(Msg,6,20) end,
    client:subscribe('client@pippo','broker1@davide', Function1 ),
	client:subscribe('client2@davide','broker2@pippo', Function2 ),
	
	timer:sleep(500),
	io:format("\n\n\n ---------------State of the brokers:---------------\n"),
	dispatcher:printState(),
	broker:printState(broker1@davide),
	broker:printState(broker2@pippo),
	
	timer:sleep(500),
    client:publish('client@pippo','broker1@davide',"Message to Broker1"),	
    client:publish('client2@davide','broker1@davide',"Ciao"),
	client:unsubscribe('client@pippo','broker1@davide',Function1),
    client:publish('client3@pippo','broker1@davide',"NO!"),
	ok.
 
treetestD()->
	io:format("\n \n *** TREETEST ROUTINE *** \n\n"),
	io:format("1) We start 3 clients on client@pippo, client2@davide, client3@pippo\n"),
	io:format("2) We start a broker on broker1@davide, directly connected to the dispatcher\n"),
	io:format("3) We start a broker on broker2@pippo, connected to broker on broker1@davide\n"),
	io:format("4) client@pippo subscribes on broker2@pippo with function nonzerolenght\n"),
	io:format("5) client2@davide subscribes on broker1@davide with function nonzerolenght\n"),
	io:format("6) client3@pippo subscribes on broker1@davide with function nonzerolenght\n"),
	io:format("7) client@pippo publishes 'Messaggio di provaaa' on broker2@pippo\n"),
	io:format("8) client2@davide publishes 'Ciaooo' on broker1@davide\n"),
	
	dispatcher:start(),
	broker:start('broker1@davide'),
	client:start('client@pippo'),
	client:start('client2@davide'),
	client:start('client3@pippo'),
	broker:start('broker2@pippo', 'broker1@davide'),
	
	F = fun(Msg) -> nonzerolenght(Msg) end,
	G = fun(Msg) -> nonzerolenght(Msg) end,
	H = fun(Msg) -> nonzerolenght(Msg) end,
	
	client:subscribe('client@pippo', 'broker2@pippo', F ),
	client:subscribe('client2@davide', 'broker1@davide', G ),
	client:subscribe('client3@pippo', 'broker1@davide', H ),
	
	timer:sleep(500),
	io:format("\n\n\n ---------------State of the brokers:---------------\n"),
	dispatcher:printState(),
	broker:printState(broker1@davide),
	broker:printState(broker2@pippo),
	
	timer:sleep(500),
	client:publish('client@pippo','broker2@pippo', "Messaggio di provaaa"),
	client:publish('client2@davide','broker1@davide', "Ciaooo"),
		
	ok.

testCallbackFunctionD()->
	testCallbackFunctionD(2).

testCallbackFunctionD(NumberOfIterations)->
	io:format("\n \n *** CALLBACK TEST ROUTINE *** \n\n"),
	io:format("0) There are two brokers directly sons of dispatcher@sara@sara. \n"),
	io:format("1) Client@pippo and client2@davide subscribe with nonzerolenght to broker1@davide and Callback function which adds ReplyTo to each recevied message which is not itself a reply. \n"),
	io:format("2) EXTERN@pippo subscribes with the nonzerolength matching function to broker2@pippo, no Callback function\n"),
	io:format("3) Client@pippo publishes a 'Msg' to broker1@davide \n"),
	io:format("4) EXTERN@pippo sees the messages in order.\n"),
	dispatcher:start(),
	broker:start('broker1@davide'),
    broker:start('broker2@pippo'),
	
	NewString = formateString(NumberOfIterations, "Re: '"),
	
	CallbackFunction = fun (Msg, Node) -> 
								Value = msgcontains(NewString,Msg), 
								case Value of
									true ->{none, none}; 
									false ->{send, "Re: '" ++ Msg ++ "' from :" ++ string:to_upper(atom_to_list(Node))} 
								end 
					   end,
								
	client:start('client@pippo', CallbackFunction),
	client:start('client2@davide', CallbackFunction),
	client:start('EXTERN@pippo'),
	
	F = fun(Msg) -> nonzerolenght(Msg) end,
	G = fun(Msg) -> nonzerolenght(Msg) end,
	H = fun(Msg) -> nonzerolenght(Msg) end,
	client:subscribe('client@pippo','broker1@davide', F ),
	client:subscribe('client2@davide','broker1@davide', G ),
	client:subscribe('EXTERN@pippo','broker2@pippo', H ),
    
	timer:sleep(500),
	io:format("\n\n\n ---------------State of the brokers:---------------\n"),
	dispatcher:printState(),
	broker:printState(broker1@davide),
	broker:printState(broker2@pippo),
	
	timer:sleep(500),
	io:format("\n\n\n ----------------Publish----------------------------\n"),
	client:publish('client@pippo','broker1@davide',"Msg"),	
   	ok.

	

%Example matching functions   

nonzerolenght(Msg)->
     string:len(Msg)=/=0.

shorterthan(Msg, Num)->
     lenghtbetween(Msg,0,Num).

lenghtbetween(Msg, Num1, Num2)->
     (string:len(Msg)>Num1) and (string:len(Msg)<Num2).

msgcontains(String,Msg) ->
    string:str(Msg, String)=/= 0.


%%
%% Local Functions
%%

formateString(NumberOfIterations, String)->
	formateString(NumberOfIterations, String, []).

formateString(0, _String, Result)->
	Result;
formateString(NumberOfIterations, String, Result) ->
	formateString(NumberOfIterations-1, String, Result ++ String).



	