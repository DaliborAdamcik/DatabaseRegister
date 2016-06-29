/**
 * 
 */
package register;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import register.exception.BadIndexException;
import register.exception.DuplicationException;
import register.exception.ValidationException;
import register.exception.WrongFormatException;

/**
 * @author Študent
 *
 */
public class DatabaseRegister implements Register, AutoCloseable {
//    public static final String URL = "jdbc:derby:C:/Users/Študent/Desktop/Derby-DBDIR/derbyDB;create=true";
//	public static final String DRIVER_CLASS = "org.apache.derby.jdbc.EmbeddedDriver";
	private final String DRIVER_CLASS = "org.apache.derby.jdbc.ClientDriver";
    private final String URL = "jdbc:derby://localhost:1527/dbreg;create=true";
    private final String USER = "dalik";
    private final String PASSWORD = "dalik";
    
    private final String creator = "CREATE TABLE persons (id integer PRIMARY KEY NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1), "
    		+ "uname VARCHAR(32) NOT NULL UNIQUE, phone VARCHAR(32) NOT NULL UNIQUE)";
    private final String insert = "INSERT INTO persons (uname, phone) VALUES (?, ?)";
    
    private Connection dbCon = null; // an connection to database  
    //private int LASTUID = 0;
    
    /**
	 * 
	 */
	public DatabaseRegister() {
		super();
		
		initConn(); // init database connection as a first, throws runtimeexception
		
        if(!execCreate(creator)) // create table persons
        	throw new RuntimeException("Cannot create table persons");
	}

	/* (non-Javadoc)
	 * @see register.Register#getCount()
	 */
	@Override
	public int getCount() {
        try(Statement stmt = dbCon.createStatement())
        {
        	ResultSet rs = stmt.executeQuery("Select count(*) as personcount from persons");
        	if(rs.next())
        		return rs.getInt(1);
        	else
        		return 0;
        } catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return 0;
		}
	}

	/* (non-Javadoc)
	 * @see register.Register#getSize()
	 */
	@Override
	public int getSize() {
		return getCount(); // maybe okay
	}

	/* (non-Javadoc)
	 * @see register.Register#getPerson(int)
	 */
	@Override
	public Person getPerson(int index) throws BadIndexException {
        try(Statement stmt = dbCon.createStatement())
        {
//        	ResultSet rs = stmt.executeQuery("Select uname, phone from persons where id = "+Integer.toString(index));
        	try(ResultSet rs = stmt.executeQuery("Select uname, phone, id from persons order by uname, phone OFFSET "+Integer.toString(index+1)+ " ROWS FETCH NEXT 1 ROWS ONLY"))
        	{
	        	if(rs.next())
	        	{        
	        		//LASTUID = rs.getInt(3);
	        		return new Person(rs.getString(1), rs.getString(2));
	        	}
	        	else
	        		throw new BadIndexException();
        	}
        } catch (SQLException | ValidationException | WrongFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return null;
	}

	/* (non-Javadoc)
	 * @see register.Register#addPerson(register.Person)
	 */
	@Override
	public void addPerson(Person person) throws DuplicationException {
        try(PreparedStatement stmt = dbCon.prepareStatement(insert))
        {
	        stmt.setString(1, person.getName());
	        stmt.setString(2, person.getPhoneNumber());
	        stmt.executeUpdate();
        } catch (SQLException e) {
        	if(e instanceof java.sql.SQLIntegrityConstraintViolationException)
        	throw new DuplicationException("Person "+person.getName()+" ("+person.getPhoneNumber()+") already exists.");	
			e.printStackTrace();
		}
	}
	
	private Person find(String name, String phone)
	{
        try(Statement stmt = dbCon.createStatement())
        {
        	try(ResultSet rs = stmt.executeQuery("Select uname, phone, id from persons WHERE "+
        			(name!=null?"uname= '"+name:"phone= '"+phone) +"'"))
        	{
	        	if(rs.next())
	        	{        
	        		//LASTUID = rs.getInt(3);
	        		return new Person(rs.getString(1), rs.getString(2));
	        	}
        	}
        } catch (SQLException | ValidationException | WrongFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return null;
		
	}

	/* (non-Javadoc)
	 * @see register.Register#findPersonByName(java.lang.String)
	 */
	@Override
	public Person findPersonByName(String name) {
		return find(name, null);
	}

	/* (non-Javadoc)
	 * @see register.Register#findPersonByPhoneNumber(java.lang.String)
	 */
	@Override
	public Person findPersonByPhoneNumber(String phoneNumber) {
		return find(null, phoneNumber);
	}

	/* (non-Javadoc)
	 * @see register.Register#removePerson(register.Person)
	 */
	@Override
	public void removePerson(Person person) {
		execNonQuerry(String.format("delete from persons where uname='%s' and phone='%s' ", person.getName(), person.getPhoneNumber()));
	}

	/* (non-Javadoc)
	 * @see register.Register#sortRegisterByName()
	 */
	@Override
	public void sortRegisterByName() { /* we dont need to do this */ }

	/* (non-Javadoc)
	 * @see register.Register#removeAllBy(char)
	 */
	@Override
	public void removeAllBy(char firstLetter) {
		execNonQuerry(String.format("delete from persons where uname like '%s", firstLetter)+"%'");
	}

	/* (non-Javadoc)
	 * @see register.Register#save()
	 */
	@Override
	public void save() {/* we directly work with database, dont need to use save */ }

	/* (non-Javadoc)
	 * @see register.Register#load()
	 */
	@Override
	public void load() { /* we directly work with database, dont need to use load */}
	
	private void initConn()
	{
        if(dbCon!= null)
        	return;
        
		try
        {
            Class.forName(DRIVER_CLASS);
        	dbCon = DriverManager.getConnection(URL, USER, PASSWORD);
        }
        catch(Exception e)
        {
        	dbCon = null; //
        	throw new RuntimeException("Can't connect to database.", e);
        }
	}
	
	private Statement getNewStatement()
	{
		try {
			return dbCon.createStatement();
		} catch (SQLException e) {
			throw new RuntimeException(e); 
		}
	}
	
	private boolean execNonQuerry(String sql)
	{
        try(Statement stmt = getNewStatement())
        {
        	stmt.executeUpdate(sql);
        	return true;
        }
        catch(Exception e)
        {
        	System.err.println(e.getMessage());
        	//e.printStackTrace();
        	return false;
        }
	}

	private boolean execCreate(String sql) // http://db.apache.org/derby/docs/10.8/ref/rrefexcept71493.html 
	{
        try(Statement stmt = getNewStatement())
        {
        	stmt.executeUpdate(sql);
        	return true;
		} catch( SQLException e ) {
			if( e.getErrorCode() == 30000 ) {  
		        return true; // That's OK (database already exists)
		    }
		    return false;//throw e;
		}
	}

	/** this is used to terminate connection on database on object destroy (automatic)
	 *  see: http://stackoverflow.com/questions/171952/is-there-a-destructor-for-java 
	 * */
	@Override
	public void close() throws Exception {
		if (dbCon!=null)
		{
			dbCon.close();
		}
	}
	

}
