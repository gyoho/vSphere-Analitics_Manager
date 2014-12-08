package logCollector;

public class Controller {

	public static void main(String[] args) throws Exception {
		Starter starter = new Starter(args[0]);
		starter.start();
	}
}
