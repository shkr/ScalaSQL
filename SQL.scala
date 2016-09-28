mport java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import scala.util.Try
import scala.util.Failure
import scala.util.Success

object SQL {

    /**
     * regex to detect delimiter.
     * ignores spaces, allows delimiter in comment, allows an equals-sign
     */
    val delimP: Pattern = Pattern.compile("^\\s*(--)?\\s*delimiter\\s*=?\\s*([^\\s]+)+\\s*.*$", Pattern.CASE_INSENSITIVE);
    
    val stopOnError: Boolean = true
    val autoCommit: Boolean = true
    
    /**
     * Runs an SQL script (read in using the Reader parameter) using the
     * connection passed in
     *
     * @param conn - the connection to use for the script
     * @param reader - the source of the script
     * @throws SQLException if any SQL errors occur
     * @throws IOException if there is an error reading from the Reader
     */
    def runScript(connection: Connection, reader: Reader): Unit = {

        var command: StringBuffer = null
        var delimiter: String = ";"
        var fullLineDelimiter: Boolean = false
             
        try {
            val lineReader = new LineNumberReader(reader)
            var line = lineReader.readLine()
          
            while (line != null) {
                if (command == null) {
                    command = new StringBuffer();
                }
                val trimmedLine = line.trim();
                val delimMatch: Matcher = delimP.matcher(trimmedLine);
                if (trimmedLine.length() < 1
                        || trimmedLine.startsWith("//")) {
                    // Do nothing
                } else if (delimMatch.matches()) {
                    delimiter = delimMatch.group(2)
                    fullLineDelimiter = false
                } else if (trimmedLine.startsWith("--")) {
                    println(trimmedLine);
                } else if (trimmedLine.length() < 1
                        || trimmedLine.startsWith("--")) {
                    // Do nothing
                } else if (!fullLineDelimiter
                        && trimmedLine.endsWith(delimiter)
                        || fullLineDelimiter
                        && trimmedLine.equals(delimiter)) {
                    command.append(line.substring(0, line
                            .lastIndexOf(delimiter)));
                    command.append(" ");
                    execCommand(connection, command, lineReader);
                    command = null;
                } else {
                    command.append(line);
                    command.append("\n");
                }
              
              line = lineReader.readLine()
            }
          
            if (command != null) {
                execCommand(connection, command, lineReader);
            }
            if (!autoCommit) {
                connection.commit();
            }
        } catch {
          case e: Exception => {
            throw new IOException(s"Error executing '${command}': ${e.getMessage}");
          }
        } finally {
            connection.rollback()
        }
    }

    def execCommand(connection: Connection, command: StringBuffer, lineReader: LineNumberReader): Unit = {
      
        val statement: Statement = connection.createStatement()

        println(command)

        var hasResults: Boolean = false
      
        Try {
            hasResults = statement.execute(command.toString())
        } match {
          case Failure(e: SQLException) =>  {
            val errText: String = s"Error executing '${command}' (line ${lineReader.getLineNumber()}): ${e.getMessage}"
            if (stopOnError) {
                throw new SQLException(errText, e)
            } else {
                println(errText)
            }
          }
          
          case Failure(e) => println(e) //Do Nothing
          
          case Success(_) => //Do Nothing
        }

        if (autoCommit && !connection.getAutoCommit()) {
            connection.commit();
        }

        val rs: ResultSet = statement.getResultSet();
        if (hasResults && rs != null) {
            val md: ResultSetMetaData = rs.getMetaData();
            val cols: Int = md.getColumnCount();
            for (i <- 1 to cols) {
                val name = md.getColumnLabel(i);
                print(name + "\t");
            }
            println("");
            while (rs.next()) {
                for (i <- 1 to cols) {
                    val value = rs.getString(i);
                    print(value + "\t");
                }
                println("");
            }
        }

        Try {
            statement.close()
        } match {
          case Failure(e) =>  // Ignore to workaround a bug in Jakarta DBCP
          case Success(_) => println("statement executed")
        }
    }
}
