package preprocessing

object TweetPreprocess {

  def dictRemoval(tweet: String): String = {

    var tweetLower = tweet.toLowerCase()
    //    https://stackoverflow.com/questions/40517562/how-to-implement-chain-of-string-replaceall-in-scala
    /*
    removes replaces smiles with happy/sad tags, decrypts abbreviation
    */
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
    // remove urls http://example.com
    val isUrl = """^((https?|ftp|smtp):\/\/)?(www.)?[a-z0-9]+\.[a-z]+(\/[a-zA-Z0-9#]+\/?)*$"""
    tweet
    .split(" ")
    .filter(x => !x.matches(isUrl))
    .mkString(" ")
  }

  def remPunct(tweet: String): String = {
    //removes all punctuation & numbers except - sign
    tweet
    .replaceAll("""[\p{Punct}&&[^-@]]""", " ")
    .filter(!_.isDigit)
    .replaceAll(" +", " ")
  }

  def stopWords(tweet: String): String = {
    // remove stop words from NLTK corpus for english language
    tweet
    .split(" ")
    .filter(x => !StopWords.stopWords.contains(x))
    .mkString(" ")
  }

  def remUsernames(tweet: String): String = {
    // remove usernemes @example
     tweet
    .split(" ")
    .filter(x => !x.contains("@"))
    .mkString(" ")
  }

  def preprocessTweetWOStopWords(tweet: String): String = {
    //same as preprocessTweet but without stop words removal
    remUsernames(remPunct(remUrl(dictRemoval(tweet)))).trim()
  }

  def preprocessTweet(tweet: String): String = {
    /*
    Apply preprocessing methods on tweet:
    1) removes replaces smiles with happy/sad tags, decrypts abbreviation
    2) remove urls
    3) remove punctuation
    4) remove usernames
    5) remove stop-words
    */
    stopWords(remUsernames(remPunct(remUrl(dictRemoval(tweet))))).trim()
  }
}
