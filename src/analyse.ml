(*
 * ==========================================================================
 *
 *           File:  Analyse History
 *
 *    Description:  Tool to fetch binance history for tokens
 *
 *         Author:  Ajhl 
 *
 * ==========================================================================
 *)

module L = List
open Core
open Async


let get_data sym =  
 printf "Getting data for %s \n" sym ;
 Rest.Depth.get ~limit:10 sym >>= begin function
  | Error err -> failwith (Rest.BinanceError.to_string err)
  | Ok { last_update_id; bids; asks } -> 
   (List.iter bids (fun l -> printf "%f - %f \n" l.p l.q));
   Deferred.unit
 end

let get_history sym = 
 printf "Getting hist data for %s \n" sym ;
 let last = 1483243199000 in
 (Rest.HistoricalData.get sym last) >>= begin function
  | Error err -> failwith (Rest.BinanceError.to_string err)
  | Ok ls -> printf "result size == %d\n" (List.length ls); Deferred.unit
 end

let process_symbol (symbol : string) =
 Rest.ExchangeInfo.get_symbols () >>= begin function
  | Error err -> failwith (Rest.BinanceError.to_string err)
  | Ok syms  -> printf "> %d\n" (L.length syms); (if (L.mem symbol syms) 
   then (get_history symbol)
   else failwith "Symbol does not exist")
 end 

let command =
 Command.async (* async_basic deprecated? *)
  ~summary:"Analyse the trade history of a symbol" 
  (let open Command.Let_syntax in
   [%map_open
    let symbol = anon ("symbol" %: string) in
    (fun () -> process_symbol symbol)])

let () = Command.run ~version:"0.1" command

