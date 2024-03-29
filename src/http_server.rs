//! The minimal HTTP server maintained by the price reporter

use std::{convert::Infallible, net::SocketAddr, sync::Arc};

use async_trait::async_trait;
use hyper::{
    server::conn::AddrStream,
    service::{make_service_fn, service_fn},
    Body, Error as HyperError, Request, Response, Server, StatusCode,
};
use matchit::Router;

use crate::{
    errors::ServerError,
    utils::{HttpRouter, UrlParams},
    ws_server::GlobalPriceStreams,
};

/// A handler is attached to a route and handles the process of translating an
/// abstract request type into a response
#[async_trait]
pub trait Handler: Send + Sync {
    /// The handler method for the request/response on the handler's route
    async fn handle(&self, req: Request<Body>, url_params: UrlParams) -> Response<Body>;
}

/// The route for the health check endpoint
const HEALTH_CHECK_ROUTE: &str = "/health";

/// The handler for the health check endpoint
pub struct HealthCheckHandler;

#[async_trait]
impl Handler for HealthCheckHandler {
    async fn handle(&self, _: Request<Body>, _: UrlParams) -> Response<Body> {
        Response::builder().status(StatusCode::OK).body(Body::from("OK")).unwrap()
    }
}

/// The HTTP server for the price reporter
#[derive(Clone)]
pub struct HttpServer {
    /// The port on which the server will listen
    port: u16,
    /// The router for the HTTP server, used to match routes
    router: Arc<HttpRouter>,
    /// A handle to the global map of price streams, used to read serve prices
    /// over HTTP
    price_streams: GlobalPriceStreams,
}

impl HttpServer {
    /// Create a new HTTP server with the given port and global price streams
    pub fn new(port: u16, price_streams: GlobalPriceStreams) -> Self {
        let router = Self::build_router();
        Self { port, router: Arc::new(router), price_streams }
    }

    /// Build the router for the HTTP server
    fn build_router() -> HttpRouter {
        let mut router: Router<Box<dyn Handler>> = Router::new();

        router.insert(HEALTH_CHECK_ROUTE, Box::new(HealthCheckHandler)).unwrap();

        router
    }

    /// Serve an http request
    async fn serve_request(&self, req: Request<Body>) -> Response<Body> {
        if let Ok(matched_path) = self.router.at(req.uri().path()) {
            let handler = matched_path.value;
            let url_params =
                matched_path.params.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
            handler.as_ref().handle(req, url_params).await
        } else {
            Response::builder().status(StatusCode::NOT_FOUND).body(Body::from("Not Found")).unwrap()
        }
    }

    /// The execution loop for the http server, accepts incoming connections,
    /// serves them, and awaits the next connection
    pub async fn execution_loop(self) -> Result<(), ServerError> {
        // Build an HTTP handler callback
        // Clone self and move it into each layer of the callback so that each
        // scope has its own copy of self
        let self_clone = self.clone();
        let make_service = make_service_fn(move |_: &AddrStream| {
            let self_clone = self_clone.clone();
            async move {
                Ok::<_, Infallible>(service_fn(move |req: Request<Body>| {
                    let self_clone = self_clone.clone();
                    async move { Ok::<_, HyperError>(self_clone.serve_request(req).await) }
                }))
            }
        });

        // Build the http server and enter its execution loop
        let addr: SocketAddr = format!("0.0.0.0:{}", self.port).parse().unwrap();
        Server::bind(&addr)
            .serve(make_service)
            .await
            .map_err(|err| ServerError::HttpServer(err.to_string()))
    }
}
