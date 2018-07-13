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

module I = Int64
module L = List
open Core
open Async

let rec get_period sym (start: int64) (endt : int64) =
 (Rest.HistoricalData.get sym start) >>= begin function
  | Error err -> failwith (Rest.BinanceError.to_string err)
  | Ok ls -> 
   let mem = L.hd (L.rev ls) in
    if (mem.close_time >= endt) then (Deferred.return ls)
    else  (get_period sym mem.close_time endt) >>= fun ll -> 
     Deferred.return (ls @ ll)
 end

let get_history sym (start : int64) = 
 let ms_time =  (Time_ns.(to_int_ns_since_epoch (now ()) / 1_000_000)) in
 let endt = Int64.of_int ms_time in
 (get_period sym start endt) >>= begin fun ls ->
  Deferred.return ls
 end

let epoch_date (stamp : int64) =
 let ns : int64 = I.mul stamp 1_000_000L in
 let ep = Time_ns.of_int_ns_since_epoch (I.to_int ns) in
 Time.to_date (Time_ns.to_time ep) Time.Zone.utc

let store (ls: Rest.HistoricalData.t) (sym:string) = 
 let fname = sym ^ ".json" in
 let open Yojson.Basic in
  let record_to_json (mem : Rest.HistoricalData.record) : json =
   `Assoc [ ("open_time", `String (I.to_string mem.open_time));    
    ("close_time", `String (I.to_string mem.close_time));    
    ("start", `Float mem.start);    
    ("high", `Float mem.high);    
    ("low", `Float mem.low);    
    ("close", `Float mem.close);    
    ("volume", `Float mem.volume);    
    ("trades", `String (I.to_string mem.trades));    
   ]
  in
  let json = `List (L.map record_to_json ls) in
  to_file fname json 




