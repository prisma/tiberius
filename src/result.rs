use crate::{
    client::Connection,
    protocol::{
        codec::DoneStatus,
        stream::{QueryStream, QueryStreamState, ReceivedToken, TokenStream},
        Context,
    },
    Row,
};
use futures::{ready, Stream, StreamExt, TryStream, TryStreamExt};
use std::{pin::Pin, sync::Arc, task};
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
/// ```
/// # use tiberius::{Client, AuthMethod};
/// # use std::env;
/// use futures::{StreamExt, TryStreamExt};
/// # async fn foo() -> Result<(), Box<dyn std::error::Error>> {
/// # let mut builder = Client::builder();
/// # if let Ok(host) = env::var("TIBERIUS_TEST_HOST") {
/// #     builder.host(host);
/// # };
/// # if let Ok(port) = env::var("TIBERIUS_TEST_PORT") {
/// #     let port: u16 = port.parse().unwrap();
/// #     builder.port(port);
/// # };
/// # if let Ok(user) = env::var("TIBERIUS_TEST_USER") {
/// #     let pw = env::var("TIBERIUS_TEST_PW").unwrap();
/// #     builder.authentication(AuthMethod::sql_server(user, pw));
/// # };
/// # let mut conn = builder.build().await?;
///
/// let mut stream = conn
///     .query(
///         "SELECT @P1; SELECT @P2",
///         &[&1i32, &2i32],
///     )
///     .await?;
///
/// // Result of `SELECT 1`. Taking the `Stream` by reference, allowing us to
/// // poll it later again.
/// let first_result: Vec<i32> = stream
///     .by_ref()
///     .map_ok(|x| x.get::<_, i32>(0))
///     .try_collect()
///     .await?;
///
/// assert_eq!(1, first_result[0]);
///
/// // Allows us to poll more results.
/// assert!(stream.next_resultset());
///
/// // Result of `SELECT 2`.
/// let second_result: Vec<i32> = stream
///     .by_ref()
///     .map_ok(|x| x.get::<_, i32>(0))
///     .try_collect()
///     .await?;
///
/// assert_eq!(2, second_result[0]);
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
pub struct QueryResult<'a> {
    stream: QueryStream<'a>,
}

impl<'a> QueryResult<'a> {
    pub(crate) fn new(
        token_stream: Box<dyn Stream<Item = crate::Result<ReceivedToken>> + 'a>,
    ) -> Self {
        let stream = QueryStream::new(token_stream);
        Self { stream }
    }

    pub(crate) async fn fetch_metadata(&mut self) -> crate::Result<()> {
        self.stream.fetch_metadata().await
    }

    /// Names of the columns of the current resultset. Order is the same as the
    /// order of columns in the rows. Needs to be called separately for every
    /// result set.
    ///
    /// ```no_run
    /// # use tiberius::{Client, AuthMethod};
    /// # use std::env;
    /// use futures::{StreamExt, TryStreamExt};
    /// # async fn foo() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut builder = Client::builder();
    /// # if let Ok(host) = env::var("TIBERIUS_TEST_HOST") {
    /// #     builder.host(host);
    /// # };
    /// # if let Ok(port) = env::var("TIBERIUS_TEST_PORT") {
    /// #     let port: u16 = port.parse().unwrap();
    /// #     builder.port(port);
    /// # };
    /// # if let Ok(user) = env::var("TIBERIUS_TEST_USER") {
    /// #     let pw = env::var("TIBERIUS_TEST_PW").unwrap();
    /// #     builder.authentication(AuthMethod::sql_server(user, pw));
    /// # };
    /// # let mut conn = builder.build().await?;
    ///
    /// let mut result_set = conn
    ///     .query(
    ///         "SELECT 1 AS foo; SELECT 2 AS bar",
    ///         &[&1i32, &2i32, &3i32],
    ///     )
    ///     .await?;
    ///
    /// assert_eq!(vec!["foo"], result_set.columns());
    /// result_set.next_resultset();
    /// assert_eq!(vec!["bar"], result_set.columns());
    /// # Ok(())
    /// # }
    /// ```
    pub fn columns(&'a self) -> Vec<&str> {
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
    pub async fn into_vec(mut self) -> crate::Result<Vec<Vec<Row>>> {
        let first: Vec<Row> = self.by_ref().try_collect().await?;
        let mut results = vec![first];

        while self.next_resultset() {
            results.push(self.by_ref().try_collect().await?);
        }

        Ok(results)
    }

    /// A convenience method on collecting the results of the first query into
    /// memory. Drops all other results.
    pub async fn into_first(self) -> crate::Result<Vec<Row>> {
        Ok(self.try_collect().await?)
    }
}

impl<'a> Stream for QueryResult<'a> {
    type Item = crate::Result<Row>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().stream).poll_next(cx)
    }
}

/// A `Stream` of counts of affected rows resulting from an `INSERT`, `UPDATE` or
/// `DELETE` query. The `ExecuteResult` needs to be polled empty before sending
/// another query to the [`Client`], failing to do so causes a flush before the
/// next query, slowing it down in an undeterministic way.
///
/// If executing multiple queries, the resulting counts will be come separately,
/// marking the rows affected for each query.
///
/// ```no_run
/// # use tiberius::{Client, AuthMethod};
/// # use std::env;
/// use futures::{StreamExt, TryStreamExt};
/// # async fn foo() -> Result<(), Box<dyn std::error::Error>> {
/// # let mut builder = Client::builder();
/// # if let Ok(host) = env::var("TIBERIUS_TEST_HOST") {
/// #     builder.host(host);
/// # };
/// # if let Ok(port) = env::var("TIBERIUS_TEST_PORT") {
/// #     let port: u16 = port.parse().unwrap();
/// #     builder.port(port);
/// # };
/// # if let Ok(user) = env::var("TIBERIUS_TEST_USER") {
/// #     let pw = env::var("TIBERIUS_TEST_PW").unwrap();
/// #     builder.authentication(AuthMethod::sql_server(user, pw));
/// # };
/// # let mut conn = builder.build().await?;
///
/// let mut stream = conn
///     .execute(
///         "INSERT INTO #Test (id) VALUES (@P1); INSERT INTO #Test (id) VALUES (@P2, @P3)",
///         &[&1i32, &2i32, &3i32],
///     )
///     .await?;
///
/// let result: Vec<u64> = stream.try_collect().await?;
/// assert_eq!(vec![1, 2], result);
/// # Ok(())
/// # }
/// ```
///
/// [`Client`]: struct.Client.html
/// [`Rows`]: struct.Row.html
/// [`next_resultset`]: #method.next_resultset
pub struct ExecuteResult<'a> {
    token_stream: Box<dyn Stream<Item = crate::Result<ReceivedToken>> + 'a>,
}

impl<'a> ExecuteResult<'a> {
    pub(crate) fn new(connection: &'a mut Connection, context: Arc<Context>) -> Self {
        let token_stream = TokenStream::new(connection, context).try_unfold();
        Self { token_stream }
    }

    /// Aggregates all resulting row counts into a sum.
    ///
    /// ```no_run
    /// # use tiberius::{Client, AuthMethod};
    /// # use std::env;
    /// use futures::{StreamExt, TryStreamExt};
    /// # async fn foo() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut builder = Client::builder();
    /// # if let Ok(host) = env::var("TIBERIUS_TEST_HOST") {
    /// #     builder.host(host);
    /// # };
    /// # if let Ok(port) = env::var("TIBERIUS_TEST_PORT") {
    /// #     let port: u16 = port.parse().unwrap();
    /// #     builder.port(port);
    /// # };
    /// # if let Ok(user) = env::var("TIBERIUS_TEST_USER") {
    /// #     let pw = env::var("TIBERIUS_TEST_PW").unwrap();
    /// #     builder.authentication(AuthMethod::sql_server(user, pw));
    /// # };
    /// # let mut conn = builder.build().await?;
    ///
    /// let stream = conn
    ///     .execute(
    ///         "INSERT INTO #Test (id) VALUES (@P1); INSERT INTO #Test (id) VALUES (@P2, @P3)",
    ///         &[&1i32, &2i32, &3i32],
    ///     )
    ///     .await?;
    ///
    /// assert_eq!(3, stream.total().await?);
    /// # Ok(())
    /// # }
    pub async fn total(self) -> crate::Result<u64> {
        self.try_fold(0, |acc, x| async move { Ok(acc + x) }).await
    }
}

impl<'a> Stream for ExecuteResult<'a> {
    type Item = crate::Result<u64>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            let stream = unsafe { Pin::new_unchecked(&mut *this.token_stream) };
            let token = ready!(stream.try_poll_next(cx)?);

            match token {
                Some(ReceivedToken::DoneProc(done)) if done.status.contains(DoneStatus::FINAL) => {
                    return Poll::Ready(None);
                }
                Some(ReceivedToken::DoneProc(done)) => {
                    return Poll::Ready(Some(Ok(done.done_rows)));
                }
                Some(ReceivedToken::DoneInProc(done)) => {
                    return Poll::Ready(Some(Ok(done.done_rows)));
                }
                Some(ReceivedToken::Done(_)) => {
                    return Poll::Ready(None);
                }
                _ => continue,
            }
        }
    }
}
