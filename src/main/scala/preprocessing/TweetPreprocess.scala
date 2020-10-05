package preprocessing

object TweetPreprocess {
//  todo - add spelll checking
//  todo add links removal from tweets

  def preprocessTweet(tweet: String): String = {
    /*
    removes user tags, replaces smiles with happy/sad tags, decrypts abbreviation
    */

    var tweetLower = tweet.toLowerCase()
    //    https://stackoverflow.com/questions/40517562/how-to-implement-chain-of-string-replaceall-in-scala

    tweetLower = ReplaceDictionaries.apostrophes2Normal.keys.foldLeft(tweetLower) { case (res, pattern) =>
      res.replaceAll(s"[^a-zA-Z]$pattern[^a-zA-Z]", " "+ReplaceDictionaries.apostrophes2Normal(pattern)+" ")
    }


    tweetLower = ReplaceDictionaries.smiles2Text.keys.foldLeft(tweetLower) { case (res, pattern) =>
      res.replaceAll(s"[^a-zA-Z]$pattern[^a-zA-Z]", " "+ReplaceDictionaries.smiles2Text(pattern)+" ")
    }

    tweetLower = ReplaceDictionaries.short2Normal.keys.foldLeft(tweetLower) { case (res, pattern) =>
      res.replaceAll(s"[^a-zA-Z]$pattern[^a-zA-Z]", " "+ReplaceDictionaries.short2Normal(pattern)+" ")
    }
    //    removes all punctuation & numbers except - sign
    tweetLower.replaceAll("""[\p{Punct}&&[^-@')(:]]""", " ").filter(!_.isDigit).
      replaceAll(" +", " ").split(" ").filter(x => !x.contains("@")).mkString(" ")
  }

}
