open Core
open Async
open Binance

module BinanceError : sig
  type t = {
    code : int ;
    msg : string ;
  }

  val encoding : t Json_encoding.encoding
  val pp : t Fmt.t
  val to_string : t -> string
end

(* Module to get the available symbols *)
module ExchangeInfo : sig
 type t = string list

  val get_symbols :
    ?buf:Bi_outbuf.t -> ?log:Log.t -> unit ->
     (t, BinanceError.t) Result.t Deferred.t
end

(* Module to get Historical Data *)
module HistoricalData : sig
 type record = { 
  open_time : int;
  close_time : int ;
  start : float ;
  high : float;
  low : float ;
  close : float;
  volume : float;
  trades : int;
 } 

 type t = record list

 type interval = Hour | Half | Quarter | Five | One

 val get :
  ?buf:Bi_outbuf.t -> ?log:Log.t -> ?interv:interval ->
  string -> int -> (t, BinanceError.t) Result.t Deferred.t
end

module Depth : sig
  type t = {
    last_update_id : int ;
    bids : Level.t list ;
    asks : Level.t list ;
  }

  val get :
    ?buf:Bi_outbuf.t -> ?log:Log.t -> ?limit:int ->
    string -> (t, BinanceError.t) Result.t Deferred.t
end

module User : sig
  module Balance : sig
    type t = {
      asset : string ;
      free : float ;
      locked : float ;
    }

    val encoding : t Json_encoding.encoding
  end

  module AccountInfo : sig
    type t = {
      makerC : int ;
      takerC : int ;
      buyerC : int ;
      sellerC : int ;
      trade : bool ;
      withdraw : bool ;
      deposit : bool ;
      timestamp : Ptime.t ;
      balances : Balance.t list ;
    }

    val encoding : t Json_encoding.encoding
    val pp : t Fmt.t
    val to_string : t -> string
  end

  module OrderStatus : sig
    type t = {
      symbol : string ;
      orderId : int ;
      clientOrderId : string ;
      price : float ;
      origQty : float ;
      executedQty : float ;
      ordStatus : OrderStatus.t ;
      timeInForce : TimeInForce.t ;
      ordType : OrderType.t ;
      side : Side.t ;
      stopPrice : float ;
      icebergQty : float ;
      time : Ptime.t ;
      isWorking : bool ;
    }

    val order_response_encoding : t Json_encoding.encoding
    val encoding : t Json_encoding.encoding
    val pp : t Fmt.t
    val to_string : t -> string
  end

  val open_orders :
    ?buf:Bi_outbuf.t -> ?log:Log.t ->
    key:string -> secret:string -> string ->
    (OrderStatus.t list, BinanceError.t) Result.t Deferred.t

  val account_info :
    ?buf:Bi_outbuf.t -> ?log:Log.t ->
    key:string -> secret:string -> unit ->
    (AccountInfo.t, BinanceError.t) Result.t Deferred.t

  val order :
    ?buf:Bi_outbuf.t ->
    ?log:Log.t ->
    ?dry_run:bool ->
    key:string ->
    secret:string ->
    symbol:string ->
    side:Side.t ->
    kind:OrderType.t ->
    ?timeInForce:TimeInForce.t ->
    qty:float ->
    ?price:float ->
    ?clientOrdID:string ->
    ?stopPx:float ->
    ?icebergQty:float -> unit ->
    (OrderStatus.t option, BinanceError.t) Result.t Deferred.t

  module Stream : sig
    val start :
      ?buf:Bi_outbuf.t -> ?log:Log.t -> key:string -> unit ->
      (string, BinanceError.t) Result.t Deferred.t
    val renew :
      ?buf:Bi_outbuf.t -> ?log:Log.t -> key:string -> string ->
      (unit, BinanceError.t) Result.t Deferred.t
    val close :
      ?buf:Bi_outbuf.t -> ?log:Log.t -> key:string -> string ->
      (unit, BinanceError.t) Result.t Deferred.t
  end
end
