package com.rbkmoney.schedulator.servlet;

import com.rbkmoney.damsel.schedule.EventSinkSrv;
import com.rbkmoney.woody.thrift.impl.http.THServiceBuilder;
import org.springframework.beans.factory.annotation.Autowired;

import javax.servlet.*;
import javax.servlet.annotation.WebServlet;
import java.io.IOException;

@WebServlet("/v1/event_sink")
public class EventSinkServlet extends GenericServlet {

    private Servlet thriftServlet;

    @Autowired
    private EventSinkSrv.Iface processorHandler;

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        thriftServlet = new THServiceBuilder()
                .build(EventSinkSrv.Iface.class, processorHandler);
    }

    @Override
    public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {
        thriftServlet.service(req, res);
    }

}
