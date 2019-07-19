using Dapper;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Z.Dapper.Plus;

namespace DapperDemos
{
    class Program
    {
        private readonly Random _rnd;
        private readonly string _connString;

        public Program()
        {
            _rnd = new Random(DateTime.Now.Second);
            _connString = ConfigurationManager.ConnectionStrings["connString"].ConnectionString;
        }

        static void Main(string[] args)
        {
            new Program().Run();
            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }

        void Run()
        {
            // mappatura 
            DapperPlusManager.Entity<Pratica>().Table("TblPraticheDataSource")
                .Identity(p => p.IdPratica);

            var generaDatiTest = true;
            
            if (generaDatiTest)
                PreparazioneDatiTest();

            Riconoscimento();

            if (generaDatiTest)
                Cleanup();
        }


        void Cleanup()
        {
            using (var dbConn = new SqlConnection(_connString))
            {
                dbConn.Execute("DROP TABLE TblFlussoPraticheMandante");
                log("Eliminata tabella TblFlussoPraticheMandante");
                dbConn.Execute("DROP TABLE TblPraticheDataSource");
                log("Eliminata tabella TblPraticheDataSource");
            }
        }
               
        private Elapseds TestRiconosci(IEnumerable<Pratica> campione, bool withTempTable, bool withIndex, bool warmup, bool bulk)
        {
            var tbName = "Tmp1";
            if (withTempTable)
                tbName = "#" + tbName;

            DapperPlusManager.Entity<FindPratica>().Table(tbName);

            var sw = new Stopwatch();

            // componiamo la nostra query di ricerca
            IEnumerable<FindPratica> found;
            var codPraticheFind = campione.Select(p => new FindPratica { CodPratica = p.CodPratica });
            long elapsedInsert = 0;
            long elapsedUpdate = 0;
            long elapsedSelect = 0;

            using (var dbConn = new SqlConnection(_connString))
            {

                dbConn.Open();

                dbConn.Execute($"CREATE TABLE {tbName} (CodPratica VARCHAR(50), IdPratica INT)");
                if (withIndex)
                    dbConn.Execute($"CREATE NONCLUSTERED INDEX IX_Tmp1_CodPratica ON {tbName} (CodPratica)");

                long checkpoint = 0;
                sw.Start();

                if (bulk)
                    dbConn.BulkInsert(codPraticheFind);
                else
                    dbConn.Execute($"INSERT INTO {tbName} (CodPratica) VALUES (@CodPratica)", codPraticheFind);

                // popola
                elapsedInsert = sw.ElapsedMilliseconds - checkpoint;
                checkpoint = sw.ElapsedMilliseconds;

                var updQuery = $@"
UPDATE t
SET IdPratica = P.Idpratica
FROM {tbName} t
INNER JOIN TblPraticheDataSource P ON P.CodPratica = t.CodPratica
";
                dbConn.Execute(updQuery);
                elapsedUpdate = sw.ElapsedMilliseconds - checkpoint;

                found = dbConn.Query<FindPratica>($"SELECT CodPratica, IdPratica FROM {tbName} WHERE IdPratica IS NOT NULL");
                elapsedSelect = sw.ElapsedMilliseconds - checkpoint;
                sw.Stop();

                dbConn.Execute($"DROP TABLE {tbName}");
            }

            return new Elapseds { ElapsedInsert = elapsedInsert, ElapsedSelect = elapsedSelect, ElapsedUpdate = elapsedUpdate };
        }

        private void Riconoscimento()
        {

            IEnumerable<Pratica> campione;
            using (var dbConn = new SqlConnection(_connString))
            {
                campione = dbConn.Query<Pratica>("SELECT * FROM TblFlussoPraticheMandante");
            }

            Console.WriteLine("Eseguo un riconoscimento di \"warmup\" senza misurarne le performance, per non penalizzare i risultati del primo test reale rispetto ai successivi.");
            TestRiconosci(campione, false, false, true, true);
            Console.WriteLine("Warmup done");

            for (var insBulk = 0; insBulk < 2; insBulk++)
            {
                var withBulk = insBulk == 0 ? false : true;
                var riconosciTipoText = withBulk ? "Bulk Insert" : "Insert tradizionale";

                Console.WriteLine($"Avvio riconoscimento pratiche con {riconosciTipoText}...");

                var mediaTempi = new List<Elapseds>();
                var cycles = 5;
                for (int i = 0; i < cycles; i++) {
                    Console.Write($"Esecuzione riconoscimento pratiche {i + 1} di {cycles}...");
                    var elapsed = TestRiconosci(campione, true, false, false, withBulk);
                    Console.WriteLine($"Elapsed for Insert {elapsed.ElapsedInsert}, Update {elapsed.ElapsedUpdate}, Select {elapsed.ElapsedSelect}, Total {elapsed.ElapsedTotal}");

                    mediaTempi.Add(elapsed);
                }

                Console.WriteLine($"Avg. Elapsed for {cycles} Insert {mediaTempi.Average(a => a.ElapsedInsert)}, Update {mediaTempi.Average(a => a.ElapsedUpdate)}, Select {mediaTempi.Average(a => a.ElapsedSelect)}, Total {mediaTempi.Average(a => a.ElapsedTotal)}");
                Console.WriteLine($"Fine riconoscimento pratiche con {riconosciTipoText}...");
            }

            Console.WriteLine("Riconoscimenti completati");
        }



        private void PreparazioneDatiTest()
        {
            log("Preparazione dati per il test");

            // 100.000 pratiche
            var pratiche = GeneraPratiche();
            log($"Generate {pratiche.Count()} pratiche con dati casuali");

            // salviamo nel db con dapper
            using (var dbConn = new SqlConnection(_connString))
            {
                var sqlCreateTblPraticheDataSource = @"

CREATE TABLE [dbo].[TblPraticheDataSource] (
    [IdPratica]  INT             IDENTITY (1, 1) NOT NULL,
    [CodPratica] VARCHAR (50)    NOT NULL,
    [DataIns]    DATETIME        NOT NULL,
    [ImportoA]   DECIMAL (18, 2) NOT NULL,
    [Note]       VARCHAR (4000)  NULL
);


CREATE NONCLUSTERED INDEX [ix_TblPraticheDataSource_codpratica]
    ON [dbo].[TblPraticheDataSource]([CodPratica] ASC);
";
                dbConn.Execute(sqlCreateTblPraticheDataSource);
                log($"Creata TblPraticheDataSource");
                dbConn.BulkInsert(pratiche);
            }
            log($"Caricate {pratiche.Count()} pratiche su TblPraticheDataSource");

            var sql2 = @"
create table TblFlussoPraticheMandante (idpratica int, codpratica varchar(50), datains datetime, importoa decimal(18,2), note varchar(4000))

INSERT INTO TblFlussoPraticheMandante (Codpratica, datains, importoA, NOTE)
SELECT Codpratica, datains, importoA, NOTE FROM 
TblPraticheDataSource
order by IdPratica 
offset 5000 rows
FETCH NEXT 30000 rows only



INSERT INTO TblFlussoPraticheMandante (Codpratica, datains, importoA, NOTE)
SELECT Codpratica, datains, importoA, NOTE FROM 
TblPraticheDataSource
order by IdPratica 
offset 40000 rows
FETCH NEXT 30000 rows only


INSERT INTO TblFlussoPraticheMandante (Codpratica, datains, importoA, NOTE)
SELECT Codpratica, datains, importoA, NOTE FROM 
TblPraticheDataSource
order by IdPratica 
offset 75000 rows
FETCH NEXT 10000 rows only
";
            using (var dbConn = new SqlConnection(_connString))
            {
                dbConn.Execute(sql2);
            }
            log($"Caricate 70.000 pratiche su TblFlussoPraticheMandante (prese da TblPraticheDataSource)");
            log("Preparazione dati per il test completata");
        }

        private IEnumerable<Pratica> GeneraPratiche()
        {
            var pratiche = new List<Pratica>();
            for (int i = 0; i < 100000; i++)
            {
                pratiche.Add(new Pratica
                {
                    IdPratica = i + 1,
                    CodPratica = RandomString(20),
                    DataIns = DateTime.Now,
                    ImportoA = (decimal)_rnd.NextDouble() * 200000M,
                    Note = RandomString(250)
                });
            }
            return pratiche;
        }

        private string RandomString(int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[_rnd.Next(s.Length)]).ToArray());
        }

        void log(string m)
        {
            Console.WriteLine(m);
        }

        class FindPratica
        {
            public string CodPratica { get; set; }
            public int? IdPratica { get; set; }
        }

        class Elapseds
        {
            public long ElapsedInsert { get; set; }
            public long ElapsedUpdate { get; set; }
            public long ElapsedSelect { get; set; }
            public long ElapsedTotal { get { return ElapsedInsert + ElapsedUpdate + ElapsedSelect; } }
        }
    }
}
