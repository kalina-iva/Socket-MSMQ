using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.OleDb;
using System.Data;
using System.Xml;
using System.Net;
using System.Net.Sockets;
using System.IO;
using System.Messaging;

namespace bdata
{
    class Program
    {
       
//        static void bdata_import()
//        {
//            string sConnStrAccess = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=1NF.mdb;";
//            var myConnectionAcc = new OleDbConnection(sConnStrAccess);
//            myConnectionAcc.Open();

//            string query = @"SELECT Country, City, [Name-stadium], Capacity, [Name-club], Coach, Founded, 
//                            [Rating-UEFA], [Surname-player], [Name-player], [Date-of-birth], Age, Growth, 
//                            Weight, [Number-of-matches], [Number-of-goals], [Position] FROM 1";
//            OleDbCommand command = new OleDbCommand(query, myConnectionAcc);
//            OleDbDataReader reader = command.ExecuteReader();
//            int i = 1;
//            while (reader.Read())
//            {
//                //country(reader);
//                //city(reader);
//                //stadium(reader);
//                //club(reader);
//                //position(reader);
//                //player(reader);
//                Console.WriteLine(i);
//                i++;
//            }
//            Console.WriteLine("Данные были успешно импортированы");
//            reader.Close();
//            myConnectionAcc.Close();
//        }
        static void Serialize(DataSet dataSet)
        {
            var memoryStream = new MemoryStream();
            dataSet.WriteXml(memoryStream, XmlWriteMode.IgnoreSchema);
            dataSet.WriteXml("input.xml");
            memoryStream.Flush();
            memoryStream.Position = 0;
        }   
        static void socket_client()
        {
            int port = 8005; // порт сервера
            string address = "127.0.0.1"; // адрес сервера    
            try
            {
                var ipPoint = new IPEndPoint(IPAddress.Parse(address), port);
                var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                // подключаемся к удаленному хосту
                socket.Connect(ipPoint);
                DataSet ds = new DataSet();
                ds = connect_access();
                Serialize(ds);
                byte[] data = File.ReadAllBytes("input.xml");

                socket.Send(data);

                // получаем ответ
                data = new byte[256]; // буфер для ответа
                StringBuilder builder = new StringBuilder();
                int bytes = 0; // количество полученных байт

                do
                {
                    bytes = socket.Receive(data, data.Length, 0);
                    builder.Append(Encoding.Unicode.GetString(data, 0, bytes));
                }
                while (socket.Available > 0);
                Console.WriteLine("ответ сервера: " + builder.ToString());

                // закрываем сокет
                socket.Shutdown(SocketShutdown.Both);
                socket.Close();

                Console.WriteLine("Данные были успешно импортированы");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
        static DataSet connect_access()
        {
            string sConnStrAccess = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=1NF.mdb;";
            var ds = new DataSet();
            using (var myConnectionAcc = new OleDbConnection(sConnStrAccess))
            {
                myConnectionAcc.Open();
                string query = @"SELECT Country, City, [Name-stadium], Capacity, [Name-club], Coach, Founded, 
                            [Rating-UEFA], [Surname-player], [Name-player], [Date-of-birth], Age, Growth, 
                            Weight, [Number-of-matches], [Number-of-goals], [Position] FROM 1";

                var adapter = new OleDbDataAdapter(query, myConnectionAcc);
                adapter.Fill(ds);
            }
            return ds;
        }
        static void msmq_client()
        {
            string path = ".\\private$\\LabaQue";
            DataSet ds = new DataSet();
            ds = connect_access();
            using (MessageQueue myQueue = new MessageQueue(path))
            {
                if (!MessageQueue.Exists(path))
                {
                    MessageQueue.Create(path);
                }
                Message _Message = new Message();
                _Message.Body = ds;
                _Message.Priority = MessagePriority.Normal;
                myQueue.Send(_Message);
                Console.WriteLine("Сообщение отправлено");
            }
        }
        static void exc_vvod(out int vybor)
        {
            try
            {
                Console.Write("Ввод: ");
                vybor = Convert.ToInt32(Console.ReadLine());
            }
            catch (Exception)
            {
                Console.WriteLine("Некорректный ввод. Повторите попытку");
                exc_vvod(out vybor);
            }
        }
        static void Main(string[] args)
        {
            bool exit = false;
            Console.Write("Отправить через:\n1. Сокеты\n2. MSMQ\n3. Выход\n");
            int vybor;
            exc_vvod(out vybor);
            do
            {
                switch (vybor)
                {
                    case 1:
                        socket_client();
                        break;
                    case 2:
                        msmq_client();
                        break;
                    case 3:
                        exit = true;
                        return;
                }  
                exc_vvod(out vybor);
            } while (!exit);
        }
    }
}
