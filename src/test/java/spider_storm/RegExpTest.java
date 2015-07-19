package spider_storm;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

public class RegExpTest {

	@Test
	public void test() throws Exception {
		Pattern compile = Pattern.compile("页面下载成功.url:\\[http://[\\w]+\\.(.+)/(.+)\\].耗时\\[(\\d+)\\]毫秒.当前时间戳:\\[(\\d+)\\]");
		Matcher matcher = compile.matcher("页面下载成功.url:[http://item.jd.com/1217499.html].耗时[1001]毫秒.当前时间戳:[1428281023774]");
		System.out.println(matcher.groupCount());
		if (matcher.find() && matcher.groupCount() == 4) {
			String url = matcher.group(1);
			String useTime = matcher.group(3);
			String currentTime = matcher.group(4);
			System.out.println(url+"--"+useTime+"--"+currentTime);
		}
	}

}
