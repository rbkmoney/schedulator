package com.rbkmoney.schedulator.servlet;

import com.rbkmoney.damsel.schedule.SchedulatorSrv;
import com.rbkmoney.woody.thrift.impl.http.THServiceBuilder;
import lombok.RequiredArgsConstructor;

import javax.servlet.*;
import javax.servlet.annotation.WebServlet;
import java.io.IOException;

@WebServlet("/v1/schedulator")
@RequiredArgsConstructor
public class SchedulatorServlet extends GenericServlet {

    private final SchedulatorSrv.Iface schedulatorHandler;
    private Servlet thriftServlet;

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        thriftServlet = new THServiceBuilder()
                .build(SchedulatorSrv.Iface.class, schedulatorHandler);
    }

    @Override
    public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {
        thriftServlet.service(req, res);
    }
}
