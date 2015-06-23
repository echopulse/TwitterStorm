<h3>Twitter Stream Processing with Apache Storm </h3>

Implements a system capable of processing incoming streams of live tweets and complete several analyses on them. 
These include identifying the following properties in a given rolling window of time: 
<ul>
  <li>Trending hashtags
  <li>The most active users 
  <li>The most referenced users (i.e. quoted in the tweet text with a “@” sign preceding their username) 
</ul>

Furthermore it attempts to identify communities between members from incoming tweets using GraphStream, a Java graphing library to visualise members in the twitter world which have mutual interests.
