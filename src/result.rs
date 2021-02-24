use crate::{
    client::Connection,
    tds::stream::{QueryStream, QueryStreamState, ReceivedToken, TokenStream},
    Column, Row,
};
use futures::{stream::BoxStream, AsyncRead, AsyncWrite, Stream, StreamExt, TryStreamExt};
use std::{fmt::Debug, pin::Pin, task};
use task::Poll;

/// A set of `Streams` of [`Rows`] resulting from a `SELECT` query. The
/// `QueryResult` needs to be polled empty before sending another query to the
/// [`Client`], failing to do so causes a flush before the next query, slowing it
/// down in an undeterministic way.
///
/// If executing multiple queries, the resulting streams will be split. Before
/// polling the next results, a call to [`next_resultset`] with a response of
/// `true` is needed. When the [`next_resultset`] returns `false` the results
/// should not be polled anymore.
///
/// # Example
///
/// ```
/// # use tiberius::Config;
/// # use tokio_util::compat::TokioAsyncWriteCompatExt;
/// # use std::env;
/// # use futures::{StreamExt, TryStreamExt};
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let c_str = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
/// #     "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
/// # );
/// # let config = Config::from_ado_string(&c_str)?;
/// # let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
/// # tcp.set_nodelay(true)?;
/// # let mut client = tiberius::Client::connect(config, tcp.compat_write()).await?;
/// let mut stream = client
///     .query(
///         "SELECT @P1 AS first; SELECT @P2 AS second",
///         &[&1i32, &2i32],
///     )
///     .await?;
///
/// // Result of `SELECT 1`. Taking the `Stream` by reference, allowing us to
/// // poll it later again. We fetch the value with column index.
/// let first_result: Vec<Option<i32>> = stream
///     .by_ref()
///     .map_ok(|x| x.get::<i32, _>(0))
///     .try_collect()
///     .await?;
///
/// assert_eq!(Some(1), first_result[0]);
///
/// // Allows us to poll more results.
/// assert!(stream.next_resultset());
///
/// // Result of `SELECT 2`, this time fetching with the column name.
/// let second_result: Vec<Option<i32>> = stream
///     .by_ref()
///     .map_ok(|x| x.get::<i32, _>("second"))
///     .try_collect()
///     .await?;
///
/// assert_eq!(Some(2), second_result[0]);
///
/// // No more results left. We should not poll again.
/// assert!(!stream.next_resultset());
/// # Ok(())
/// # }
/// ```
///
/// [`Client`]: struct.Client.html
/// [`Rows`]: struct.Row.html
/// [`next_resultset`]: #method.next_resultset
#[derive(Debug)]
pub struct QueryResult<'a> {
    stream: QueryStream<'a>,
}

impl<'a> QueryResult<'a> {
    pub(crate) fn new(token_stream: BoxStream<'a, crate::Result<ReceivedToken>>) -> Self {
        let stream = QueryStream::new(token_stream);

        Self { stream }
    }

    pub(crate) async fn fetch_metadata(&mut self) -> crate::Result<()> {
        self.stream.fetch_metadata().await
    }

    /// Names of the columns of the current resultset. Order is the same as the
    /// order of columns in the rows. Needs to be called separately for every
    /// result set. None if query could not result anything.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use tiberius::Config;
    /// # use tokio_util::compat::TokioAsyncWriteCompatExt;
    /// # use std::env;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let c_str = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
    /// #     "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
    /// # );
    /// # let config = Config::from_ado_string(&c_str)?;
    /// # let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
    /// # tcp.set_nodelay(true)?;
    /// # let mut client = tiberius::Client::connect(config, tcp.compat_write()).await?;
    /// let mut result_set = client
    ///     .query(
    ///         "SELECT 1 AS foo; SELECT 2 AS bar",
    ///         &[&1i32, &2i32, &3i32],
    ///     )
    ///     .await?;
    ///
    /// let columns = result_set.columns().unwrap();
    /// assert_eq!("foo", columns[0].name());
    ///
    /// result_set.next_resultset();
    ///
    /// let columns = result_set.columns().unwrap();
    /// assert_eq!("bar", columns[0].name());
    /// # Ok(())
    /// # }
    /// ```
    pub fn columns(&'a self) -> Option<&[Column]> {
        self.stream.columns()
    }

    /// Returns `true` if stream has more result sets available. Must be called
    /// before polling again to get results from the next query.
    pub fn next_resultset(&mut self) -> bool {
        if self.stream.state == QueryStreamState::Initial {
            true
        } else if self.stream.state == QueryStreamState::HasNext {
            self.stream.state = QueryStreamState::Initial;

            true
        } else {
            false
        }
    }

    /// Collects results from all queries in the stream into memory in the order
    /// of querying.
    pub async fn into_results(mut self) -> crate::Result<Vec<Vec<Row>>> {
        if self.stream.state == QueryStreamState::Done {
            Ok(Vec::new())
        } else {
            let first: Vec<Row> = self.by_ref().try_collect().await?;
            let mut results = vec![first];

            while self.next_resultset() {
                results.push(self.by_ref().try_collect().await?);
            }

            Ok(results)
        }
    }

    /// Collects the output of the first query, dropping any further
    /// results.
    pub async fn into_first_result(self) -> crate::Result<Vec<Row>> {
        let mut results = self.into_results().await?.into_iter();
        let rows = results.next().unwrap_or(Vec::new());

        Ok(rows)
    }

    /// Collects the first row from the output of the first query, dropping any
    /// further rows.
    pub async fn into_row(self) -> crate::Result<Option<Row>> {
        let mut results = self.into_first_result().await?.into_iter();

        Ok(results.next())
    }
}

impl<'a> Stream for QueryResult<'a> {
    type Item = crate::Result<Row>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().stream).poll_next(cx)
    }
}

/// A result from a query execution, listing the number of affected rows.
///
/// If executing multiple queries, the resulting counts will be come separately,
/// marking the rows affected for each query.
///
/// # Example
///
/// ```no_run
/// # use tiberius::Config;
/// # use tokio_util::compat::TokioAsyncWriteCompatExt;
/// # use std::env;
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let c_str = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
/// #     "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
/// # );
/// # let config = Config::from_ado_string(&c_str)?;
/// # let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
/// # tcp.set_nodelay(true)?;
/// # let mut client = tiberius::Client::connect(config, tcp.compat_write()).await?;
/// let result = client
///     .execute(
///         "INSERT INTO #Test (id) VALUES (@P1); INSERT INTO #Test (id) VALUES (@P2, @P3)",
///         &[&1i32, &2i32, &3i32],
///     )
///     .await?;
///
/// assert_eq!(&[1, 2], result.rows_affected());
/// # Ok(())
/// # }
/// ```
///
/// [`Client`]: struct.Client.html
/// [`Rows`]: struct.Row.html
/// [`next_resultset`]: #method.next_resultset
#[derive(Debug)]
pub struct ExecuteResult {
    rows_affected: Vec<u64>,
}

impl<'a> ExecuteResult {
    pub(crate) async fn new<S: AsyncRead + AsyncWrite + Unpin + Send>(
        connection: &'a mut Connection<S>,
    ) -> crate::Result<Self> {
        let token_stream = TokenStream::new(connection).try_unfold();

        let rows_affected = token_stream
            .try_fold(Vec::new(), |mut acc, token| async move {
                match token {
                    ReceivedToken::DoneProc(done) if done.is_final() => (),
                    ReceivedToken::DoneProc(done) => acc.push(done.rows()),
                    ReceivedToken::DoneInProc(done) => acc.push(done.rows()),
                    _ => (),
                }
                Ok(acc)
            })
            .await?;

        Ok(Self { rows_affected })
    }

    /// A slice of numbers of rows affected in the same order as the given
    /// queries.
    pub fn rows_affected(&self) -> &[u64] {
        self.rows_affected.as_slice()
    }

    /// Aggregates all resulting row counts into a sum.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use tiberius::Config;
    /// # use tokio_util::compat::TokioAsyncWriteCompatExt;
    /// # use std::env;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let c_str = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
    /// #     "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
    /// # );
    /// # let config = Config::from_ado_string(&c_str)?;
    /// # let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
    /// # tcp.set_nodelay(true)?;
    /// # let mut client = tiberius::Client::connect(config, tcp.compat_write()).await?;
    /// let rows_affected = client
    ///     .execute(
    ///         "INSERT INTO #Test (id) VALUES (@P1); INSERT INTO #Test (id) VALUES (@P2, @P3)",
    ///         &[&1i32, &2i32, &3i32],
    ///     )
    ///     .await?;
    ///
    /// assert_eq!(3, rows_affected.total());
    /// # Ok(())
    /// # }
    pub fn total(self) -> u64 {
        self.rows_affected.into_iter().sum()
    }
}

impl IntoIterator for ExecuteResult {
    type Item = u64;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows_affected.into_iter()
    }
}
