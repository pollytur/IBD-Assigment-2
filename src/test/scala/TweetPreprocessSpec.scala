import org.scalatest.{FlatSpec, Matchers}
import preprocessing.TweetPreprocess

class TweetPreprocessSpec extends FlatSpec with Matchers {
  "preprocess tweet" should "delete user mentions" in {
    val tweet = "@user when a father is dysfunctional"
    val expected = "when a father is dysfunctional"

    TweetPreprocess.preprocessTweet(tweet) shouldBe expected
  }

  "preprocess tweet" should "delete tags" in {
    val tweet = "@user @user thanks for #lyft"
    val expected = "thanks for lyft"

    TweetPreprocess.preprocessTweet(tweet) shouldBe expected
  }

  "preprocess tweet" should "remove punctuation" in {
    val tweet = "we won!!! love the land!!!"
    val expected = "we won love the land"

    TweetPreprocess.preprocessTweet(tweet) shouldBe expected
  }

  "preprocess tweet" should "unwrap shortcut words" in {
    val tweet = "i love u take with u all the time"
    val expected = "i love you take with you all the time"

    TweetPreprocess.preprocessTweet(tweet) shouldBe expected
  }

  "preprocess tweet" should "remove digits and special symbols" in {
    val tweet = "[2/2] huge fan fare and big talking before"
    val expected = "huge fan fare and big talking before"

    TweetPreprocess.preprocessTweet(tweet) shouldBe expected
  }

  "preprocess tweet" should "remove links" in {
    val tweet = "look here https://test.com"
    val expected = "look here"

    TweetPreprocess.preprocessTweet(tweet) shouldBe expected
  }
}
