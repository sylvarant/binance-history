(*
 * ==========================================================================
 *
 *           File:  History
 *
 *    Description:  Help query the rest api to build the history
 *
 *         Author:  Ajhl 
 *
 * ==========================================================================
 *)

open Async
open Core

val get_history : string -> int64 -> Rest.HistoricalData.t Deferred.t

val epoch_date : int64 -> Date.t  

val store : Rest.HistoricalData.t -> string -> unit

