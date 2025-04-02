read -p "Enter the year (YYYY): " y
read -p "Enter the month (MM): " m

unzstd -c lichess_db_standard_rated_$y-$m.pgn.zst | 
  awk -v OFS=',' '
    BEGIN {
      print "username,time_control,rating,entry_seq" > "temp_data.csv"
      entry_seq = 0
    }
    
    /^\[Event / { in_game = 1 }
    /^$/ && in_game {
      if (tc_category != 0 && white != "" && black != "" && white_elo+0 > 0 && black_elo+0 > 0) {
        print white, tc_category, white_elo, entry_seq >> "temp_data.csv"
        print black, tc_category, black_elo, entry_seq >> "temp_data.csv"
      }
      entry_seq++
      white = black = white_elo = black_elo = tc_category = ""
      in_game = 0
      next
    }
    
    in_game {
      if (/^\[White "/) {
        split($0, parts, "\"")
        white = parts[2]
      }
      else if (/^\[Black "/) {
        split($0, parts, "\"")
        black = parts[2]
      }
      else if (/^\[WhiteElo "/) {
        split($0, parts, "\"")
        white_elo = parts[2]
      }
      else if (/^\[BlackElo "/) {
        split($0, parts, "\"")
        black_elo = parts[2]
      }
      else if (/^\[TimeControl "/) {
        split($0, parts, "\"")
        split(parts[2], tc, "+")
        est = tc[1] + tc[2] * 40
        
        if (est < 30)        tc_category = 0
        else if (est < 180)  tc_category = 1
        else if (est < 480)  tc_category = 2
        else if (est < 1500) tc_category = 3
        else                tc_category = 0
      }
    }
  '
