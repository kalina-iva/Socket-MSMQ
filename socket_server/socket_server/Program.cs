using System;
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.Data.SqlClient;
using System.Data;
using System.Xml;
using System.IO;
using System.Messaging;

namespace SocketTcpServer
{
    class Program
    {
        static string sConnSql = new SqlConnectionStringBuilder
        {
            DataSource = @"PC\SQLEXPRESS",
            InitialCatalog = "FC for app",
            IntegratedSecurity = true
        }.ConnectionString;
        static void loading_data(DataSet dataSet)
        {
            foreach (DataTable dt in dataSet.Tables)
            {
                using (var reader = dt.CreateDataReader())
                {
                    while (reader.Read())
                    {
                        country(reader);
                        city(reader);
                        stadium(reader);
                        club(reader);
                        position(reader);
                        player(reader);
                    }
                }
            }
        }
        static void country(DataTableReader reader)
        {
            using (var sConn = new SqlConnection(sConnSql))
            {
                sConn.Open();
                var sCommand = new SqlCommand
                {
                    Connection = sConn,
                    CommandText = @"IF NOT EXISTS (SELECT * FROM Country WHERE [name-country] = @name_country)
                                                INSERT INTO Country([name-country]) VALUES (@name_country)"
                };
                sCommand.Parameters.AddWithValue("@name_country", reader[0]);
                try
                {
                    sCommand.ExecuteNonQuery();
                }
                catch (Exception e) { }
            }
        }
        static void city(DataTableReader reader)
        {
            using (var sConn = new SqlConnection(sConnSql))
            {
                sConn.Open();
                var sCommand = new SqlCommand
                {
                    Connection = sConn,
                    CommandText = @"IF NOT EXISTS (SELECT * FROM City WHERE [name-city] = @name_city AND [id-country]=(SELECT [id-country]
                                                   FROM Country WHERE [name-country] = @name_country))
	                                    INSERT INTO City([name-city], [id-country])
                                        VALUES (@name_city, (SELECT [id-country]
                                                   FROM Country WHERE [name-country] = @name_country))"
                };
                sCommand.Parameters.AddWithValue("@name_city", reader[1]);
                sCommand.Parameters.AddWithValue("@name_country", reader[0]);
                try
                {
                    sCommand.ExecuteNonQuery();
                }
                catch (Exception e) { }
            }
        }
        static void stadium(DataTableReader reader)
        {
            using (var sConn = new SqlConnection(sConnSql))
            {
                sConn.Open();
                var sCommand = new SqlCommand
                {
                    Connection = sConn,
                    CommandText = @"IF NOT EXISTS (SELECT * FROM Stadium WHERE [name-stadium] = @name_stadium AND [id-city]=(SELECT [id-city]
                                                   FROM City WHERE [name-city] = @name_city))
                                    INSERT INTO Stadium([name-stadium], capacity, [id-city])
                                     VALUES (@name_stadium, @capacity, (SELECT [id-city]
                                                   FROM City WHERE [name-city] = @name_city))"
                };
                sCommand.Parameters.AddWithValue("@name_stadium", reader[2]);
                sCommand.Parameters.AddWithValue("@capacity", reader[3]);
                sCommand.Parameters.AddWithValue("@name_city", reader[1]);
                try
                {
                    sCommand.ExecuteNonQuery();
                }
                catch (Exception e) { }
            }
        }
        static void club(DataTableReader reader)
        {
            using (var sConn = new SqlConnection(sConnSql))
            {
                sConn.Open();
                var sCommand = new SqlCommand
                {
                    Connection = sConn,
                    CommandText = @"IF NOT EXISTS (SELECT * FROM Club WHERE [name-club] = @name_club AND [coach]=@coach)
                                        INSERT INTO Club([name-club], coach, founded, [rating-uefa], [id-stadium])
                                        VALUES (@name_club, @coach, @founded, @rating_uefa, (SELECT [id-stadium]
                                                   FROM Stadium WHERE [name-stadium] = @name_st))"
                };
                sCommand.Parameters.AddWithValue("@name_club", reader[4]);
                sCommand.Parameters.AddWithValue("@coach", reader[5]);
                sCommand.Parameters.AddWithValue("@founded", reader[6]);
                sCommand.Parameters.AddWithValue("@rating_uefa", reader[7]);
                sCommand.Parameters.AddWithValue("@name_st", reader[2]);
                try
                {
                    sCommand.ExecuteNonQuery();
                }
                catch (Exception e) { }
            }
        }
        static void position(DataTableReader reader)
        {
            using (var sConn = new SqlConnection(sConnSql))
            {
                sConn.Open();
                var sCommand = new SqlCommand
                {
                    Connection = sConn,
                    CommandText = @"IF NOT EXISTS (SELECT * FROM Position WHERE [name-position] = @name_pos)
                                        INSERT INTO Position([name-position]) 
                                            VALUES (@name_pos)"
                };
                sCommand.Parameters.AddWithValue("@name_pos", reader[16]);
                try
                {
                    sCommand.ExecuteNonQuery();
                }
                catch (Exception e) { }
            }
        }
        static void player(DataTableReader reader)
        {
            using (var sConn = new SqlConnection(sConnSql))
            {
                sConn.Open();
                var sCommandCity = new SqlCommand
                {
                    Connection = sConn,
                    CommandText = @"IF NOT EXISTS (SELECT * FROM Player WHERE [surname-player] = @surname AND [date-birthday]=@date_b)
                                            INSERT INTO Player([surname-player], [name-player], [date-birthday],
                                                 age, growth, weight, [id-club], [number-of-matches], [number-of-goals], [id-position] )
                                            VALUES (@surname, @name_p, @date_b, @age, @growth, @weight, 
                                            (SELECT [id-club] FROM Club WHERE [name-club] = @name_cl), 
                                            @num_matches, @num_goals, (SELECT [id-position]
                                                            FROM Position WHERE [name-position] = @name_pos))"
                };
                sCommandCity.Parameters.AddWithValue("@surname", reader[8]);
                sCommandCity.Parameters.AddWithValue("@name_p", reader[9]);
                sCommandCity.Parameters.AddWithValue("@date_b", reader[10]);
                sCommandCity.Parameters.AddWithValue("@age", reader[11]);
                sCommandCity.Parameters.AddWithValue("@growth", reader[12]);
                sCommandCity.Parameters.AddWithValue("@weight", reader[13]);
                sCommandCity.Parameters.AddWithValue("@name_cl", reader[4]);
                sCommandCity.Parameters.AddWithValue("@num_matches", reader[14]);
                sCommandCity.Parameters.AddWithValue("@num_goals", reader[15]);
                sCommandCity.Parameters.AddWithValue("@name_pos", reader[16]);
                try
                {
                    sCommandCity.ExecuteNonQuery();
                }
                catch (Exception e) { }
            }
        }
        static DataSet receive_msmq()
        {
            MessageQueue myQueue = new MessageQueue(".\\private$\\LabaQue");

            // Set the queue to read the priority. By default, it
            // is not read.
            myQueue.MessageReadPropertyFilter.Priority = true;

            // Set the formatter to indicate body contains a string.
            myQueue.Formatter = new XmlMessageFormatter(new Type[] { typeof(DataSet) });

            try
            {
                // Receive and format the message. 
                Message myMessage = myQueue.Receive();
                DataSet ds = (DataSet)myMessage.Body;            
                Console.WriteLine("Priority: " +
                    myMessage.Priority.ToString());

                //Console.WriteLine(DateTime.Now.ToLongTimeString() + ": Данные загружаются в БД...");
                //loading_data(ds);
                //Console.WriteLine(DateTime.Now.ToLongTimeString() + ": Данные были успешно загружены в БД");

                //Console.Read();
                return ds;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return null;
            }
        }
        static int port = 8005; // порт для приема входящих запросов
        static DataSet receive_socket()
        {
            IPEndPoint ipPoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), port);
            Socket listenSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            try
            {
                listenSocket.Bind(ipPoint);
                listenSocket.Listen(10);
                Console.WriteLine("Сервер запущен. Ожидание подключений...");
                var dataSet = new DataSet();
                while (true)
                {
                    Socket handler = listenSocket.Accept();
                    byte[] data = new byte[32768]; // буфер для получаемых данных

                    do
                    {
                        handler.Receive(data);
                        using (var fs = new MemoryStream())
                        {
                            fs.Write(data, 0, data.Length);
                            fs.Position = 0;
                            dataSet.ReadXml(fs);
                        }
                    }
                    while (handler.Available > 0);

                    Console.WriteLine(DateTime.Now.ToLongTimeString() + ": Данные получены");

                    //Console.WriteLine(DateTime.Now.ToLongTimeString() + ": Данные загружаются в БД...");
                    //loading_data(dataSet);
                    //Console.WriteLine(DateTime.Now.ToLongTimeString() + ": Данные были успешно загружены в БД");

                    // отправляем ответ
                    string message = "Ваше сообщение доставлено с помощью сокетов в " + DateTime.Now.ToShortTimeString();
                    data = Encoding.Unicode.GetBytes(message);
                    handler.Send(data);
                    // закрываем сокет
                    handler.Shutdown(SocketShutdown.Both);
                    handler.Close();
                    return dataSet;
                }               
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return null;
            }
        }
        static void Main(string[] args)
        {
            DataSet dataSet = new DataSet();
            //dataSet = receive_socket();
            dataSet = receive_msmq();
            Console.WriteLine(DateTime.Now.ToLongTimeString() + ": Данные загружаются в БД...");
            loading_data(dataSet);
            Console.WriteLine(DateTime.Now.ToLongTimeString() + ": Данные были успешно загружены в БД");
            Console.Read();
        }
    }
}