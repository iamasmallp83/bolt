package com.cmex.bolt.spot.rest;

import com.cmex.bolt.spot.domain.Account;
import com.cmex.bolt.spot.service.AccountDispatcher;
import com.cmex.bolt.spot.service.AccountService;
import com.cmex.bolt.spot.service.OrderDispatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class SpotController {

    private final List<AccountDispatcher> accountDispatchers;
    private final List<OrderDispatcher> orderDispatchers;

    @Autowired
    public SpotController(List<AccountDispatcher> accountDispatchers, List<OrderDispatcher> orderDispatchers) {
        this.accountDispatchers = accountDispatchers;
        this.orderDispatchers = orderDispatchers;
    }

    @GetMapping("/")
    public String index() {
        return "bolt manager rest api";
    }

    @GetMapping("/accounts/{id}")
    public Account getAccount(@PathVariable int id) {
        return accountDispatchers.get(id % 10).getAccountService().getAccount(id);
    }
}
