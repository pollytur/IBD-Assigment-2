package preprocessing

object TweetPreprocess {

  def dictRemoval(tweet: String): String = {
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
    tweetLower
  }

  def remUrl(tweet: String): String = {
    var isUrl="""^((https?|ftp|smtp):\/\/)?(www.)?[a-z0-9]+\.[a-z]+(\/[a-zA-Z0-9#]+\/?)*$"""
    tweet
    .split(" ")
    .filter(x => !x.matches(isUrl))
    .mkString(" ")
  }

  def remPunct(tweet: String): String = {
    tweet
    .replaceAll("""[\p{Punct}&&[^-@]]""", " ")
    .filter(!_.isDigit)
    .replaceAll(" +", " ")
  }

  def stopWords(tweet: String): String = {
    tweet
    .split(" ")
    .filter(x => !x.contains("@"))
    .filter(x => !StopWords.stopWords.contains(x))
    .mkString(" ")
  }

  def preprocessTweet(tweet: String): String = {
    /*
    removes user tags, replaces smiles with happy/sad tags, decrypts abbreviation
    */
    //    removes all punctuation & numbers except - sign
    stopWords(remPunct(remUrl(dictRemoval(tweet))))
  }
}
