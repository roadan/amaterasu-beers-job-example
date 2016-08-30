import com.flyberrycapital.slack.SlackClient

val s = new SlackClient("xoxp-73743425940-73697828947-73755088949-dc73460073")

s.chat.postMessage("#demos", env.configuration("err-msg").toString)
