
public class Controller {

	public static void main(String[] args) throws Exception {
		/*MapperTest test = new MapperTest();
		test.test();*/
		
		Starter starter = new Starter(args[0]);
		starter.start();
	}
}
